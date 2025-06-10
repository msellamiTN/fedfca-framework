import json
import time
import logging
import uuid
import base64
import yaml
import os
from confluent_kafka import Producer, Consumer, KafkaError
import socket
from concurrent.futures import ThreadPoolExecutor
import threading

# Assuming these custom modules are in the path
from commun.logactor import LoggerActor  # For structured logging
from commun.FedFcaMonitor import FedFcaMonitor  # For performance monitoring
from commun.ConceptStabilityAnalyzer import ConceptStabilityAnalyzer
from commun.CryptoManager import CryptoManager
from fedfca_core import FedFCA_core as core

import redis

class ALMActor:
    """
    Actor for Local Model (ALM) in FedFCA.
    Integrates with FedFCA_core.Provider for local lattice computation and encryption.
    """
    
    def __init__(self, kafka_servers=None, max_workers=10):
        # Initialize logger first - use environment variable or generate unique ID
        self.actor_id = f"ALM_{os.environ.get('ACTOR_ID_SUFFIX', uuid.uuid4().hex[:6])}"
        logging.basicConfig(level=logging.DEBUG, format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(self.actor_id)
        
        # Initialize metrics logging
        self.logactor = LoggerActor(self.actor_id)
        
        # Load config
        self.config_file_path = "/data/config.yml"
        self.config = self._load_config()
        self.logger.info(f"Config: {self.config}")
        
        # Use Kafka servers from config if not provided
        self.kafka_servers = kafka_servers or self.config.get("kafka", {}).get("bootstrap_servers", "kafka-1:19092,kafka-2:19093,kafka-3:19094")
        self.logger.info(f"Kafka servers: {self.kafka_servers}")
        # Initialize Redis connection
        redis_host = os.getenv("REDIS_HOST", "datastore")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        try:
            self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            self.redis_client.ping()
            self.logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None        
        # Initialize components
        self.data_encryption_service = core.Encryption()
        self.lattice_builder = core.FasterFCA(
            threshold=self.config.get("provider", {}).get("threshold", 0.1),
            context_sensitivity=self.config.get("provider", {}).get("context_sensitivity", 0.5)
        )

        self.quality_analyzer = ConceptStabilityAnalyzer()
        self.monitor = FedFcaMonitor()
        self.federation_channel_crypto = CryptoManager()
        
        # Standardize topic names
        self.federation_init_topic = "federation.init"
        self.federation_response_topic = "federation.response"
        self.negotiation_keys_topic = "negotiation.keys"
        self.key_acknowledgment_topic = "key.acknowledgment"
        self.participant_lattice_topic = "participant.lattice"
        self.key_distribution_topic = f"key.distribution.{self.actor_id}"
        self.key_request_global_topic = "key.request.global"
        self.results_distribution_topic = "results.distribution"
        self.results_acknowledgment_topic = "results.acknowledgment"
        self.alm_topic = f"alm_{self.actor_id}"

        # Configure Kafka producer
        producer_config = {
            'bootstrap.servers': self.kafka_servers,
            'message.max.bytes': 10485880,
            # 'linger.ms': 10,
            # 'retries': 3,
            # 'retry.backoff.ms': 1000,
            # 'client.id': f"producer_{self.actor_id}",
            # 'security.protocol': 'PLAINTEXT',
            # 'debug': 'all',
            # 'socket.timeout.ms': 10000,
            # 'socket.keepalive.enable': True,
            # 'reconnect.backoff.ms': 1000,
            # 'reconnect.backoff.max.ms': 5000
        }
        self.logger.debug(f"Producer config: {producer_config}")
        self.producer = Producer(producer_config)
        
        # Configure Kafka consumer
        consumer_config = {
            'bootstrap.servers': self.kafka_servers,
            'group.id': f"alm_consumer_group_{self.actor_id}",
            'auto.offset.reset': 'earliest',
            # 'enable.auto.commit': True,
            # 'client.id': f"consumer_{self.actor_id}",
            # 'security.protocol': 'PLAINTEXT',
            # 'debug': 'all',
            # 'socket.timeout.ms': 10000,
            # 'socket.keepalive.enable': True,
            # 'reconnect.backoff.ms': 1000,
            # 'reconnect.backoff.max.ms': 5000,
            # 'session.timeout.ms': 30000,
            # 'heartbeat.interval.ms': 10000
        }
        self.logger.debug(f"Consumer config: {consumer_config}")
        self.consumer = Consumer(consumer_config)
        
        # Subscribe to all required topics
        self.subscribed_topics = [
            self.federation_init_topic,
            self.federation_response_topic,
            self.negotiation_keys_topic,
            self.key_distribution_topic,
            self.key_acknowledgment_topic,
            self.participant_lattice_topic,
            self.key_request_global_topic,
            self.results_distribution_topic,
            self.results_acknowledgment_topic,
            self.alm_topic
        ]
        
        self.logger.info(f"Subscribing to topics: {self.subscribed_topics}")
        self.consumer.subscribe(self.subscribed_topics)
        
        self.logger.info(f"ALMActor {self.actor_id} initialized. Listening on: {self.subscribed_topics}")
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Federation state
        self.active_federations = {}  # {fed_id: {'config': {}, 'status': '', 'round': 0, ...}}
        self.federation_metrics = {}  # {fed_id: {'lattice_size': len, 'quality_metrics': {...}}}
        self.federation_keys = {}  # {fed_id: {'K1': key, 'K3': key, 'K5': key}}
        
        # Provider index - can be used to identify specific providers
        self.provider_index = int(os.environ.get('PROVIDER_INDEX', 0))
        
        # Register with federation system - both in Redis and via Kafka
        self._register_with_redis()
        self._register_with_federation()

    def _register_with_federation(self):
        """Register this ALM with the federation system."""
        try:
            # Create provider metadata
            provider_metadata = {
                "action": "provider_register",
                "provider_id": self.actor_id,
                "status": "ready",
                "capabilities": {
                    "max_workers": self.executor._max_workers,
                    "provider_index": self.provider_index,
                    "datasize": self._estimate_data_capacity()  # Utilise une méthode pour estimer la capacité de données
                },
                "timestamp": time.time()
            }
            
            # Send registration message to federation_init topic
            max_retries = 3
            for retry in range(max_retries):
                try:
                    self.producer.produce(
                        self.federation_init_topic,
                        value=json.dumps(provider_metadata).encode('utf-8')
                    )
                    self.producer.flush()
                    self.logger.info(f"Registered provider {self.actor_id} with federation system")
                    break
                except Exception as e:
                    if retry < max_retries - 1:
                        self.logger.warning(f"Failed to register with federation system (attempt {retry + 1}/{max_retries}): {e}")
                        time.sleep(2)
                    else:
                        self.logger.error(f"Failed to register with federation system after {max_retries} attempts: {e}")
            
        except Exception as e:
            self.logger.error(f"Error registering with federation system: {e}")

    def _register_with_redis(self):
        """Register this ALM with Redis for participant discovery"""
        self.logger.info(f"Registering provider {self.actor_id} with Redis")
        if not self.redis_client:
            self.logger.warning("Redis not available, skipping Redis registration")
            return
        try:
            # Create provider metadata
            provider_data = {
                "provider_id": self.actor_id,
                "status": "active",
                "capabilities": json.dumps({
                    "datasize": self._estimate_data_capacity(),
                    "max_workers": self.executor._max_workers,
                    "provider_index": self.provider_index
                }),
                "timestamp": str(time.time())
            }
            
            # Utiliser hset au lieu de hmset (déprécié)
            redis_key = f"participant:{self.actor_id}"
            for key, value in provider_data.items():
                self.redis_client.hset(redis_key, key, value)
            
            self.redis_client.expire(redis_key, 3600)  # Expire after 1 hour
            self.logger.info(f"Registered provider {self.actor_id} with Redis under '{redis_key}'")
        except Exception as e:
            self.logger.error(f"Error registering provider with Redis: {e}")

    def _estimate_data_capacity(self):
        """Estimate the data capacity of this ALM based on available data"""
        # À implémenter : logique pour estimer la capacité réelle basée sur les données disponibles
        # Pour l'instant, retourne une valeur par défaut
        return 1000  # Valeur par défaut

    def _load_config(self):
        """Load configuration from YAML file."""
        try:
            with open(self.config_file_path, 'r') as file:
                config_data = yaml.safe_load(file)
                self.logger.info(f"Configuration loaded from {self.config_file_path}")
                return config_data if config_data else {}
        except FileNotFoundError:
            self.logger.warning(f"Config file not found at {self.config_file_path}. Using defaults.")
            # Create default config
            default_config = {
                "kafka": {
                    "bootstrap_servers": self.kafka_servers,
                    "topics": {
                        "alm_submissions": "alm_submissions",
                        "agm_control": "agm_control"
                    }
                },
                "provider": {
                    "threshold": 0.1,
                    "context_sensitivity": 0.5,
                    "privacy_preserving": True
                }
            }
            # Try to save default config
            try:
                os.makedirs(os.path.dirname(self.config_file_path), exist_ok=True)
                with open(self.config_file_path, 'w') as file:
                    yaml.dump(default_config, file)
                self.logger.info(f"Default configuration created at {self.config_file_path}")
                return default_config
            except Exception as e:
                self.logger.error(f"Error creating default config: {e}")
        except Exception as e:
            self.logger.error(f"Error loading config: {e}")
        return {}  # Return empty dict if error or not found

    def _save_metrics_to_redis(self, federation_id, metrics_dict):
        """Save federation metrics to Redis."""
        if not self.redis_client:
            self.logger.warning("Redis client not available. Metrics not saved.")
            return
            
        try:
            # Convert any non-string values to strings for Redis
            string_metrics = {}
            for key, value in metrics_dict.items():
                if isinstance(value, dict):
                    string_metrics[key] = json.dumps(value)
                else:
                    string_metrics[key] = str(value)
                    
            metrics_key = f"provider:{self.actor_id}:federation:{federation_id}:metrics"
            self.redis_client.hmset(metrics_key, string_metrics)
            self.redis_client.expire(metrics_key, 3600 * 24)  # Expire after 24 hours
            self.logger.info(f"Saved metrics for federation {federation_id} to Redis")
        except Exception as e:
            self.logger.error(f"Error saving metrics to Redis: {e}")

    def _load_provider_data(self, data_file_path):
        """Load provider data from file."""
        try:
            if not os.path.exists(data_file_path):
                self.logger.error(f"Data file not found: {data_file_path}")
                return None
                
            with open(data_file_path, 'r') as f:
                data = f.read()
                
            # Parse data based on format
            if data_file_path.endswith('.txt'):
                # Assume FIMI format
                return self._parse_fimi_data(data)
            else:
                self.logger.error(f"Unsupported data format for file: {data_file_path}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error loading provider data: {e}")
            return None

    def _parse_fimi_data(self, data):
        """Parse data in FIMI format."""
        try:
            # Split into lines and remove empty lines
            lines = [line.strip() for line in data.split('\n') if line.strip()]
            
            # Parse each line into a set of items
            transactions = []
            for line in lines:
                items = set(map(int, line.split()))
                if items:  # Only add non-empty transactions
                    transactions.append(items)
                    
            return transactions
            
        except Exception as e:
            self.logger.error(f"Error parsing FIMI data: {e}")
            return None

    def _ensure_topics_exist(self):
        """Ensure that all required Kafka topics exist"""
        max_retries = 5
        retry_delay = 2  # seconds
        
        for retry in range(max_retries):
            try:
                # Create admin client with proper configuration
                admin_client = Producer({
                    'bootstrap.servers': self.kafka_servers,
                    'client.id': f"admin_{self.actor_id}",
                    'message.max.bytes': 10485880,
                    'retries': 3,
                    'retry.backoff.ms': 1000,
                    'security.protocol': 'PLAINTEXT'
                })
                
                # First, try to create topics
                for topic in self.subscribed_topics:
                    try:
                        # Create topic with replication factor and partitions
                        admin_client.produce(
                            topic,
                            value=json.dumps({
                                "action": "topic_init",
                                "config": {
                                    "replication.factor": 1,
                                    "num.partitions": 1
                                }
                            }).encode('utf-8'),
                            on_delivery=self._delivery_callback
                        )
                        self.logger.info(f"Topic {topic} creation attempted")
                    except KafkaError as ke:
                        self.logger.warning(f"Error creating topic {topic}: {ke}")
                    except Exception as e:
                        self.logger.warning(f"Error creating topic {topic}: {e}")
                
                admin_client.flush(timeout=5)
                
                # Then verify topics exist by trying to produce a test message
                for topic in self.subscribed_topics:
                    try:
                        admin_client.produce(
                            topic,
                            value=json.dumps({"action": "topic_verify"}).encode('utf-8'),
                            on_delivery=self._delivery_callback
                        )
                        self.logger.info(f"Topic {topic} verification message sent")
                    except KafkaError as ke:
                        self.logger.warning(f"Error verifying topic {topic}: {ke}")
                        if retry < max_retries - 1:
                            self.logger.info(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            continue
                    except Exception as e:
                        self.logger.warning(f"Error verifying topic {topic}: {e}")
                        if retry < max_retries - 1:
                            self.logger.info(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            continue
                
                admin_client.flush(timeout=5)
                self.logger.info("Topic verification completed")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to ensure topics exist (attempt {retry + 1}/{max_retries}): {e}")
                if retry < max_retries - 1:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Failed to ensure topics exist after all retries")
                    return False
                    
    def _delivery_callback(self, err, msg):
        """Callback for Kafka delivery confirmations"""
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            self.logger.debug(f"Message delivered to {topic}[{partition}]@{offset}")

    def _process_incoming_message(self, message_data):
        """Processes a message received on ALM's topics."""
        try:
            action = message_data.get("action")
            self.logger.info(f"DÉBUT TRAITEMENT MESSAGE: action={action}")
            
            # Vérification des messages à ignorer d'abord
            if action in ["topic_verify", "topic_init"]:
                self.logger.debug(f"Ignoring verification message with action: {action}")
                return True
            
            # Traiter explicitement les messages provider_register
            if action == "provider_register":
                self.logger.info(f"ÉTAPE: Traitement message d'enregistrement de fournisseur: {message_data.get('provider_id')}")
                return self._process_provider_registration(message_data)
            
            # Traitement par type d'action
            if action == "federation_init":
                self.logger.info(f"ÉTAPE: Traitement invitation de fédération: {message_data.get('federationID') or message_data.get('federation_id')}")
                return self._handle_federation_invite(message_data)
            
            elif action == "federation_response":
                self.logger.info(f"ÉTAPE: Traitement réponse de fédération: {message_data.get('federation_id')}")
                # Si cet ALM est l'initiateur de la fédération, traiter la réponse
                federation_id = message_data.get('federation_id')
                if federation_id in self.active_federations and self.active_federations[federation_id].get('initiator') == self.actor_id:
                    participant_id = message_data.get('participant_id')
                    status = message_data.get('status')
                    self.logger.info(f"Reçu réponse {status} de {participant_id} pour fédération {federation_id}")
                    
                    # Stocker la réponse
                    if 'responses' not in self.active_federations[federation_id]:
                        self.active_federations[federation_id]['responses'] = {}
                    self.active_federations[federation_id]['responses'][participant_id] = status
                    
                    # Vérifier si toutes les réponses ont été reçues
                    participants = self.active_federations[federation_id].get('participants', [])
                    if len(self.active_federations[federation_id]['responses']) >= len(participants):
                        self.logger.info(f"Toutes les réponses reçues pour la fédération {federation_id}")
                else:
                    self.logger.info(f"Ignorer la réponse de fédération (pas l'initiateur): {federation_id}")
                return True
            
            elif action == "key_negotiation":
                self.logger.info(f"ÉTAPE: Négociation des clés pour la fédération: {message_data.get('federation_id')}")
                return self._handle_key_negotiation(message_data)
            
            elif action == "key_distribution":
                self.logger.info(f"ÉTAPE: Distribution des clés pour la fédération: {message_data.get('federation_id')}")
                return self._handle_key_distribution(message_data)
            
            elif action == "dataset_config":
                self.logger.info(f"ÉTAPE: Configuration du dataset pour la fédération: {message_data.get('federation_id')}")
                return self._handle_dataset_configuration(message_data)
            
            elif action == "key_request_global":
                self.logger.info(f"ÉTAPE: Demande de clés globales pour la fédération: {message_data.get('federation_id')}")
                return self._handle_key_request_global(message_data)
            
            elif action == "results_distribution":
                self.logger.info(f"ÉTAPE: Distribution des résultats pour la fédération: {message_data.get('federation_id')}")
                return self._handle_results_distribution(message_data)
            
            elif action == "shutdown_alm":
                self.logger.info("ÉTAPE: Ordre d'arrêt reçu")
                return False
            
            # Cas des messages inconnus
            self.logger.warning(f"ÉTAPE: Action inconnue reçue: {action}")
            return True
            
        except Exception as e:
            self.logger.error(f"ERREUR dans le traitement du message: {e}", exc_info=True)
            return True

    def _handle_federation_invite(self, message_data):
        """Handle invitation to join a federation - Event 1"""
        # L'AGM envoie federationID mais nous attendons federation_id
        federation_id = message_data.get("federationID") or message_data.get("federation_id")
        selected_participants = message_data.get("selectedParticipants", [])
        parameters = message_data.get("parameters", {})
        
        self.logger.info(f"Received federation invite: {message_data}")
        
        if not all([federation_id, selected_participants]):
            self.logger.warning(f"Invalid federation invite: Missing fields. Message: {message_data}")
            return True
        
        try:
            # Check if we are selected
            if self.actor_id not in selected_participants:
                self.logger.info(f"Not selected for federation {federation_id}")
                return True
            
            # Send acceptance message
            response_message = {
                'action': 'federation_response',
                'federation_id': federation_id,
                'participant_id': self.actor_id,
                'status': 'ACCEPT',
                'timestamp': time.time()
            }
            
            self.logger.info(f"Sending federation response: {response_message}")
            
            self.producer.produce(
                self.federation_response_topic,  # Utiliser le bon topic pour la réponse
                value=json.dumps(response_message).encode('utf-8')
            )
            self.producer.flush()
            
            # Initialize federation state
            self.active_federations[federation_id] = {
                'status': 'invited',
                'parameters': parameters,
                'start_time': time.time(),
                'round': 0
            }
            
            self.logger.info(f"Accepted invitation to federation {federation_id}")
            
        except Exception as e:
            self.logger.error(f"Error processing federation invite: {e}", exc_info=True)
            
        return True

    def _handle_key_negotiation(self, message_data):
        """Handle key negotiation protocol - Event 2"""
        federation_id = message_data.get("federation_id")
        key_parameters = message_data.get("keyParameters")
        
        if not all([federation_id, key_parameters]):
            self.logger.warning("Invalid key negotiation message: Missing fields")
            return True
            
        try:
            # Generate key pair for this federation
            key_pair = self.federation_channel_crypto.generate_key_pair()
            
            # Store the key pair
            if federation_id not in self.federation_keys:
                self.federation_keys[federation_id] = {}
            self.federation_keys[federation_id]['key_pair'] = key_pair
            
            # Send key acknowledgment
            ack_message = {
                'action': 'key_acknowledgment',
                'federation_id': federation_id,
                'participant_id': self.actor_id,
                'key_receipt_id': str(uuid.uuid4()),
                'signature': self.federation_channel_crypto.sign_message(key_parameters)
            }
            
            self.producer.produce(
                self.key_acknowledgment_topic,
                value=json.dumps(ack_message).encode('utf-8')
            )
            self.producer.flush()
            
            self.logger.info(f"Completed key negotiation for federation {federation_id}")
            
        except Exception as e:
            self.logger.error(f"Error in key negotiation: {e}", exc_info=True)
            
        return True

    def _handle_key_distribution(self, message_data):
        """Handle key distribution message - Event 2"""
        federation_id = message_data.get("federation_id")
        encrypted_k1 = message_data.get("encryptedK1")
        signature_agm = message_data.get("signatureAGM")
        
        if not all([federation_id, encrypted_k1, signature_agm]):
            self.logger.warning("Invalid key distribution message: Missing fields")
            return True
            
        try:
            # Verify AGM signature
            if not self.federation_channel_crypto.verify_signature(encrypted_k1, signature_agm):
                self.logger.error("Invalid AGM signature in key distribution")
                return True
            
            # Decrypt K1 using our private key
            k1 = self.federation_channel_crypto.decrypt_with_private_key(
                encrypted_k1,
                self.federation_keys[federation_id]['key_pair']['private']
            )
            
            # Store K1
            self.federation_keys[federation_id]['K1'] = k1
            
            # Send acknowledgment
            ack_message = {
                'action': 'key_acknowledgment',
                'federation_id': federation_id,
                'participant_id': self.actor_id,
                'key_receipt_id': str(uuid.uuid4()),
                'signature': self.federation_channel_crypto.sign_message(encrypted_k1)
            }
            
            self.producer.produce(
                self.key_acknowledgment_topic,
                value=json.dumps(ack_message).encode('utf-8')
            )
            self.producer.flush()
            
            self.logger.info(f"Received and stored K1 for federation {federation_id}")
            
        except Exception as e:
            self.logger.error(f"Error in key distribution: {e}", exc_info=True)
            
        return True

    def _handle_key_request_global(self, message_data):
        """Handle global key request from GS - Event 5"""
        federation_id = message_data.get("federation_id")
        key_request_id = message_data.get("keyRequestID")
        signature_gs = message_data.get("signatureGS")
        
        if not all([federation_id, key_request_id, signature_gs]):
            self.logger.warning("Invalid key request message: Missing fields")
            return True
            
        try:
            # Verify GS signature
            if not self.federation_channel_crypto.verify_signature(key_request_id, signature_gs):
                self.logger.error("Invalid GS signature in key request")
                return True
            
            # Get stored keys
            k1 = self.federation_keys[federation_id]['K1']
            k3 = self.federation_keys[federation_id].get('K3')
            
            # Encrypt keys for transmission
            encrypted_k1 = self.federation_channel_crypto.encrypt_with_public_key(k1)
            encrypted_k3 = self.federation_channel_crypto.encrypt_with_public_key(k3) if k3 else None
            
            # Send key response
            response_message = {
                'action': 'key_response_global',
                'federation_id': federation_id,
                'participant_id': self.actor_id,
                'encryptedK1': encrypted_k1,
                'encryptedK3': encrypted_k3,
                'signature': self.federation_channel_crypto.sign_message(key_request_id)
            }
            
            self.producer.produce(
                self.key_request_global_topic,
                value=json.dumps(response_message).encode('utf-8')
            )
            self.producer.flush()
            
            self.logger.info(f"Sent key response for federation {federation_id}")
            
        except Exception as e:
            self.logger.error(f"Error in key request handling: {e}", exc_info=True)
            
        return True

    def _handle_results_distribution(self, message_data):
        """Handle results distribution from GS - Event 6"""
        federation_id = message_data.get("federation_id")
        encrypted_results = message_data.get("encryptedResults")
        encrypted_k5 = message_data.get("encryptedK5")
        signature_gs = message_data.get("signatureGS")
        
        if not all([federation_id, encrypted_results, encrypted_k5, signature_gs]):
            self.logger.warning("Invalid results distribution message: Missing fields")
            return True
            
        try:
            # Verify GS signature
            if not self.federation_channel_crypto.verify_signature(encrypted_results, signature_gs):
                self.logger.error("Invalid GS signature in results distribution")
                return True
            
            # Decrypt K5
            k5 = self.federation_channel_crypto.decrypt_with_private_key(
                encrypted_k5,
                self.federation_keys[federation_id]['key_pair']['private']
            )
            
            # Store K5
            self.federation_keys[federation_id]['K5'] = k5
            
            # Decrypt results
            results = self.federation_channel_crypto.decrypt_with_symmetric_key(
                encrypted_results,
                k5
            )
            
            # Process results
            self._process_federation_results(federation_id, results)
            
            # Send acknowledgment
            ack_message = {
                'action': 'results_acknowledgment',
                'federation_id': federation_id,
                'participant_id': self.actor_id,
                'result_receipt_id': str(uuid.uuid4()),
                'signature': self.federation_channel_crypto.sign_message(encrypted_results)
            }
            
            self.producer.produce(
                self.results_acknowledgment_topic,
                value=json.dumps(ack_message).encode('utf-8')
            )
            self.producer.flush()
            
            self.logger.info(f"Processed results for federation {federation_id}")
            
        except Exception as e:
            self.logger.error(f"Error in results distribution: {e}", exc_info=True)
            
        return True

    def _process_federation_results(self, federation_id, results):
        """Process and store federation results"""
        try:
            # Update federation status
            if federation_id in self.active_federations:
                self.active_federations[federation_id]['status'] = 'complete'
                self.active_federations[federation_id]['results'] = results
                self.active_federations[federation_id]['completion_time'] = time.time()
            
            # Store metrics
            self.federation_metrics[federation_id]['completion_time'] = time.time()
            self.federation_metrics[federation_id]['results'] = results
            
            self.logger.info(f"Stored results for federation {federation_id}")
            
        except Exception as e:
            self.logger.error(f"Error processing federation results: {e}", exc_info=True)

    def _handle_round_start(self, message_data):
        """Handle start of a federation round"""
        federation_id = message_data.get("federation_id")
        round_num = message_data.get("round", 0)
        
        if federation_id not in self.active_federations:
            self.logger.warning(f"Round start for unknown federation {federation_id}")
            return True
            
        try:
            federation = self.active_federations[federation_id]
            federation['round'] = round_num
            federation['status'] = 'round_in_progress'
            federation['round_start_time'] = time.time()
            
            self.logger.info(f"Starting round {round_num} for federation {federation_id}")
            
            # Start monitoring
            self.monitor.start_monitoring()
            
            # Get local lattice
            local_lattice = federation.get('local_lattice')
            if not local_lattice:
                self.logger.error(f"No local lattice available for federation {federation_id}")
                return True
            
            # Encrypt the lattice for submission
            self.logger.info(f"Encrypting lattice for submission to AGM")
            encrypted_lattice = self.federation_channel_crypto.encrypt_data(local_lattice)
            lattice_payload_b64 = base64.b64encode(encrypted_lattice).decode('utf-8')
            
            # Stop monitoring
            self.monitor.stop_monitoring()
            encryption_metrics = self.monitor.get_metrics()
            
            # Prepare submission metrics
            submission_metrics = {
                'round': round_num,
                'original_size': len(local_lattice),
                'encrypted_size': len(encrypted_lattice),
                'encryption_time': encryption_metrics.get('duration', 0)
            }
            
            # Send lattice to AGM
            self.logger.info(f"Submitting encrypted lattice to AGM for federation {federation_id}, round {round_num}")
            submission_message = {
                'action': 'lattice_submission',
                'provider_id': self.actor_id,
                'federation_id': federation_id,
                'lattice_payload': lattice_payload_b64,
                'round': round_num,
                'metrics': submission_metrics,
                'signature': self.federation_channel_crypto.sign_message(lattice_payload_b64)
            }
            
            self.producer.produce(
                self.participant_lattice_topic,
                value=json.dumps(submission_message).encode('utf-8')
            )
            self.producer.flush()
            
            # Update federation status
            federation['status'] = 'submitted'
            federation['submission_time'] = time.time()
            
            # Log metrics
            round_metrics = {
                "action": "round_submission",
                "federation_id": federation_id,
                "provider_id": self.actor_id,
                "round": round_num,
                "lattice_size": len(local_lattice),
                "encrypted_size": len(encrypted_lattice),
                "encryption_time": encryption_metrics.get('duration', 0),
                "timestamp": time.time()
            }
            self.logactor.log_metrics(round_metrics)
            
            self.logger.info(f"Successfully submitted lattice for federation {federation_id}, round {round_num}")
            
        except Exception as e:
            self.logger.error(f"Error processing round start: {e}", exc_info=True)
            
        return True

    def _handle_round_complete(self, message_data):
        """Handle completion of a federation round"""
        federation_id = message_data.get("federation_id")
        round_num = message_data.get("round", 0)
        global_stability = message_data.get("global_stability", 0.0)
        
        if federation_id not in self.active_federations:
            self.logger.warning(f"Round complete for unknown federation {federation_id}")
            return True
            
        try:
            federation = self.active_federations[federation_id]
            
            # Update federation status
            federation['status'] = 'ready'  # Ready for next round
            federation['last_round_complete'] = time.time()
            federation['global_stability'] = global_stability
            
            self.logger.info(f"Round {round_num} complete for federation {federation_id}. Global stability: {global_stability}")
            
            # Log metrics
            completion_metrics = {
                "action": "round_completed",
                "federation_id": federation_id,
                "provider_id": self.actor_id,
                "round": round_num,
                "global_stability": global_stability,
                "timestamp": time.time()
            }
            self.logactor.log_metrics(completion_metrics)
            
        except Exception as e:
            self.logger.error(f"Error processing round complete: {e}", exc_info=True)
            
        return True

    def _handle_dataset_configuration(self, message_data):
        """Handle dataset configuration and build lattice - Event 3"""
        federation_id = message_data.get("federation_id")
        dataset_config = message_data.get("dataset_config", {})
        encryption_key_b64 = message_data.get("encryption_key_b64")
        
        self.logger.info(f"Processing dataset configuration for federation: {federation_id}")
        
        if not federation_id:
            self.logger.warning("Invalid dataset configuration: Missing federation ID")
            return False
        
        try:
            # Initialize federation entry if not exists
            if federation_id not in self.active_federations:
                self.active_federations[federation_id] = {
                    'status': 'configuring',
                    'round': 0
                }
            
            # Set default configuration if not provided
            if not dataset_config:
                dataset_config = {
                    "input_file": "/data/federated_datasets/mushroom.dat",
                    "threshold": 0.1,
                    "context_sensitivity": 0.5
                }
                self.logger.info(f"Using default configuration: {dataset_config}")
            
            # Store configuration
            self.active_federations[federation_id]['dataset_config'] = dataset_config
            
            # Handle encryption key
            if encryption_key_b64:
                try:
                    encryption_key = base64.b64decode(encryption_key_b64)
                    if federation_id not in self.federation_keys:
                        self.federation_keys[federation_id] = {}
                    self.federation_keys[federation_id]['dataset_key'] = encryption_key
                except Exception as e:
                    self.logger.error(f"Error decoding encryption key: {e}")
                    return False
            
            # Process dataset and build lattice
            input_file = dataset_config.get("input_file")
            if not input_file or not os.path.exists(input_file):
                self.logger.error(f"Input file not found: {input_file}")
                return False
            
            # Load and validate data
            try:
                # Initialize FasterFCA with current parameters
                threshold = float(dataset_config.get("threshold", 0.1))
                context_sensitivity = float(dataset_config.get("context_sensitivity", 0.5))
                
                self.lattice_builder = core.FasterFCA(
                    threshold=threshold,
                    context_sensitivity=context_sensitivity
                )
                
                # Process the file and build lattice
                output_file = f"/tmp/lattice_{federation_id}.json"
                local_lattice = self.lattice_builder.run(input_file, output_file)
                
                if not local_lattice:
                    self.logger.error("Failed to generate lattice")
                    return False
                
                # Store the lattice
                self.active_federations[federation_id]['local_lattice'] = local_lattice
                self.logger.info(f"Successfully built lattice with {len(local_lattice)} concepts")
                
                # Encrypt and submit the lattice
                return self._submit_lattice(federation_id, local_lattice)
                
            except Exception as e:
                self.logger.error(f"Error processing dataset: {str(e)}", exc_info=True)
                return False
                
        except Exception as e:
            self.logger.error(f"Error in dataset configuration: {str(e)}", exc_info=True)
            return False
            
    def _submit_lattice(self, federation_id, local_lattice):
        """Helper method to encrypt and submit the lattice"""
        try:
            # Get encryption key
            key = None
            if federation_id in self.federation_keys:
                key = self.federation_keys[federation_id].get('K1') or \
                      self.federation_keys[federation_id].get('dataset_key')
            
            # Generate a temporary key if none available
            if not key:
                key = self.federation_channel_crypto.generate_symmetric_key()
                self.logger.info("Generated temporary key for lattice encryption")
                if federation_id not in self.federation_keys:
                    self.federation_keys[federation_id] = {}
                self.federation_keys[federation_id]['temp_key'] = key
            
            # Encrypt the lattice
            encrypted_lattice = self.federation_channel_crypto.encrypt_with_symmetric_key(
                json.dumps(local_lattice),
                key
            )
            
            # Prepare metadata
            lattice_metadata = {
                'provider_id': self.actor_id,
                'lattice_size': len(local_lattice),
                'encryption_time': time.time(),
                'threshold': self.active_federations[federation_id]['dataset_config'].get('threshold', 0.1),
                'context_sensitivity': self.active_federations[federation_id]['dataset_config'].get('context_sensitivity', 0.5)
            }
            
            # Sign the message
            signature = self.federation_channel_crypto.sign_message(encrypted_lattice)
            
            # Prepare submission message
            submission_message = {
                'action': 'lattice_submission',
                'federation_id': federation_id,
                'participant_id': self.actor_id,
                'encrypted_lattice': base64.b64encode(encrypted_lattice).decode('utf-8'),
                'lattice_metadata': lattice_metadata,
                'signature': signature,
                'timestamp': time.time()
            }
            
            # Send to AGM
            self.producer.produce(
                self.participant_lattice_topic,
                value=json.dumps(submission_message).encode('utf-8')
            )
            self.producer.flush()
            
            self.logger.info(f"Submitted encrypted lattice for federation {federation_id}")
            
            # Update metrics
            if federation_id not in self.federation_metrics:
                self.federation_metrics[federation_id] = {}
                
            self.federation_metrics[federation_id].update({
                'lattice_size': len(local_lattice),
                'encryption_time': time.time(),
                'submission_time': time.time()
            })
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error submitting lattice: {str(e)}", exc_info=True)
            return False
            
        except Exception as e:
            self.logger.error(f"Error processing dataset configuration: {e}", exc_info=True)
        
        return True

    def _finalize_participant_list(self, federation_id):
        """Finalize the participant list for a federation and start key negotiation"""
        if federation_id not in self.active_federations:
            self.logger.warning(f"Cannot finalize participant list for unknown federation {federation_id}")
            return
        
        federation = self.active_federations[federation_id]
        
        # Filter only accepted participants
        accepted_participants = [
            p_id for p_id, response in self.participant_responses[federation_id].items()
            if response == "ACCEPT"
        ]
        
        # Update federation status
        federation['final_participants'] = accepted_participants
        federation['status'] = 'participants_finalized'
        
        self.logger.info(f"Finalized participant list for federation {federation_id}: {accepted_participants}")
        
        # If we have enough participants, start key negotiation protocol
        min_participants = federation['parameters'].get('min_participants', 2)
        if len(accepted_participants) >= min_participants:
            self.logger.info(f"Starting key negotiation for federation {federation_id}")
            self._start_key_negotiation(federation_id)
            
            # Programmer l'envoi des configurations de dataset peu après la négociation des clés
            threading.Timer(10.0, self._distribute_dataset_configurations, args=[federation_id]).start()
            
        else:
            self.logger.warning(f"Not enough participants accepted. Federation {federation_id} cancelled.")
            federation['status'] = 'cancelled'
            # In a real implementation, we would notify the initiator here

    def start_listening(self):
        """Starts the main Kafka message listening loop for ALM."""
        self.logger.info(f"ALM {self.actor_id} actively listening on...{self.subscribed_topics}")
        
        # Ensure topics exist with retries
        max_topic_retries = 5
        topic_retry_delay = 5  # seconds
        
        for retry in range(max_topic_retries):
            if self._ensure_topics_exist():
                break
            if retry < max_topic_retries - 1:
                self.logger.info(f"Retrying topic creation in {topic_retry_delay} seconds...")
                time.sleep(topic_retry_delay)
            else:
                self.logger.error("Failed to create topics after all retries")
                return

        try:
            while True:
                self.logger.debug("Polling for messages...")
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None: 
                    self.logger.debug("No message received in this poll")
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF: 
                        self.logger.debug("Reached end of partition")
                        continue
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        self.logger.warning(f"Topic doesn't exist yet. Waiting...")
                        time.sleep(2)
                        continue
                    else:
                        self.logger.error(f"Kafka consume error: {msg.error()}")
                        # Try to reconnect
                        try:
                            self.consumer.close()
                            consumer_config = {
                                'bootstrap.servers': self.kafka_servers,
                                'group.id': f"alm_consumer_group_{self.actor_id}",
                                'auto.offset.reset': 'earliest',
                                'enable.auto.commit': True,
                                'client.id': f"consumer_{self.actor_id}",
                                'security.protocol': 'PLAINTEXT',
                                'debug': 'all',
                                'socket.timeout.ms': 10000,
                                'socket.keepalive.enable': True,
                                'reconnect.backoff.ms': 1000,
                                'reconnect.backoff.max.ms': 5000,
                                'session.timeout.ms': 30000,
                                'heartbeat.interval.ms': 10000
                            }
                            self.logger.debug(f"Reconnecting with config: {consumer_config}")
                            self.consumer = Consumer(consumer_config)
                            self.consumer.subscribe(self.subscribed_topics)
                            self.logger.info(f"Reconnected to topics: {self.subscribed_topics}")
                            continue
                        except Exception as reconnect_err:
                            self.logger.error(f"Reconnection failed: {reconnect_err}")
                            break
                
                try:
                    # Check if message is empty
                    if not msg.value() or len(msg.value()) == 0:
                        self.logger.debug("Empty message received, ignored")
                        continue
                        
                    message_data = json.loads(msg.value().decode('utf-8'))
                    action = message_data.get("action")
                    
                    # Ne filtrer que les messages de vérification, pas les messages d'enregistrement
                    if action in ["topic_init", "topic_verify"]:
                        self.logger.debug(f"Filtered message: {action} on {msg.topic()}")
                        continue
                    
                    # Log pour les autres types de messages
                    self.logger.info(f"Received message on topic {msg.topic()}: {action}")
                    
                    # Traiter le message
                    if not self._process_incoming_message(message_data):
                        break
                    
                except json.JSONDecodeError:
                    self.logger.warning(f"Non-JSON message ignored: {msg.value()}")
                    continue
                
        except KeyboardInterrupt:
            self.logger.info("ALM listening loop interrupted.")
        finally:
            self.stop_actor()

    def stop_actor(self):
        """Gracefully stops the ALM actor."""
        self.logger.info("ALM stopping...")
        
        # Stop any monitoring in progress
        if hasattr(self, 'monitor'):
            if hasattr(self.monitor, 'is_monitoring') and self.monitor.is_monitoring():
                self.monitor.stop_monitoring()
            else:
                # Si la méthode is_monitoring n'existe pas, essayer d'appeler stop_monitoring directement
                try:
                    self.monitor.stop_monitoring()
                    self.logger.info("Monitor stopped")
                except Exception as e:
                    self.logger.warning(f"Could not stop monitoring: {e}")
            
            try:
                self.logger.info(f"ALM Final Monitoring Data: {self.monitor.get_metrics()}")
            except Exception as e:
                self.logger.warning(f"Could not get monitoring metrics: {e}")
        
        # Save final metrics for all federations
        for federation_id, metrics in self.federation_metrics.items():
            metrics['final_timestamp'] = time.time()
            self._save_metrics_to_redis(federation_id, metrics)
        
        # Close Kafka resources
        try:
            self.consumer.close()
            self.producer.flush(timeout=5)  # Wait up to 5s for outstanding messages
        except Exception as e:
            self.logger.error(f"Error during Kafka cleanup: {e}")
            
        # Shutdown thread pool
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)
            
        # Log shutdown
        try:
            shutdown_metrics = {
                "action": "alm_shutdown",
                "actor_id": self.actor_id,
                "timestamp": time.time()
            }
            if hasattr(self.logactor, 'log_metrics'):
                self.logactor.log_metrics(shutdown_metrics)
            else:
                self.logger.info(f"Shutdown metrics: {shutdown_metrics}")
        except Exception as e:
            self.logger.warning(f"Could not log shutdown metrics: {e}")
            
        self.logger.info(f"ALM {self.actor_id} has stopped.")

    def _process_provider_registration(self, message_data):
        """Process provider registration message"""
        provider_id = message_data.get("provider_id")
        status = message_data.get("status")
        capabilities = message_data.get("capabilities", {})
        timestamp = message_data.get("timestamp")
        
        if not all([provider_id, status]):
            self.logger.warning(f"Invalid provider registration: Missing required fields. Data: {message_data}")
            return True
        
        self.logger.info(f"Processing provider registration: Provider {provider_id} with status {status}")
        self.logger.info(f"Provider capabilities: {capabilities}")
        
        # Store provider information in Redis with proper TTL
        if self.redis_client:
            try:
                provider_key = f"provider:{provider_id}"
                provider_data = {
                    "provider_id": provider_id,
                    "status": status,
                    "capabilities": json.dumps(capabilities),
                    "last_seen": str(timestamp)
                }
                
                self.logger.info(f"Storing provider data in Redis at key: {provider_key}")
                
                for key, value in provider_data.items():
                    self.redis_client.hset(provider_key, key, value)
                
                self.redis_client.expire(provider_key, 3600)  # 1 hour TTL
                self.logger.info(f"Successfully saved provider {provider_id} to Redis with 1 hour TTL")
                
                # Vérifier si nous avons assez de fournisseurs pour initier une fédération
                participant_keys = self.redis_client.keys("provider:*")
                num_providers = len(participant_keys)
                self.logger.info(f"Found {num_providers} registered providers, checking for federation initialization")
                
                # Vérifier si nous avons au moins 2 participants et qu'aucune fédération n'est active
                # Et que notre ID actuel est le premier dans l'ordre lexicographique (pour éviter les doublons)
                if num_providers >= 2 and not self.active_federations:
                    # Récupérer tous les IDs des fournisseurs et les trier
                    provider_ids = []
                    for key in participant_keys:
                        provider_id_key = key.decode('utf-8').split(':', 1)[1] if isinstance(key, bytes) else key.split(':', 1)[1]
                        provider_ids.append(provider_id_key)
                    
                    provider_ids.sort()
                    
                    # Si nous sommes le "leader" (premier par ordre lexicographique), nous initialisons la fédération
                    if provider_ids[0] == self.actor_id:
                        self.logger.info(f"This ALM ({self.actor_id}) is the leader. Initializing federation.")
                        
                        # Préparation des paramètres de la fédération
                        context_id = f"auto_federation_{int(time.time())}"
                        parameters = {
                            "min_participants": 2,
                            "required_capabilities": {},
                            "differential_privacy": {"epsilon": 1.0, "delta": 0.0001},
                            "dataset_config": {
                                "input_file": "/data/federated_datasets/mushroom.dat",
                                "threshold": 0.1,
                                "context_sensitivity": 0.5
                            }
                        }
                        
                        # Créer une fédération
                        federation_id = self.request_federation(context_id, parameters)
                        if federation_id:
                            self.logger.info(f"AUTO-FEDERATION INITIATED: {federation_id}")
                            
                            # Prévoir l'envoi des configurations de dataset après un délai
                            threading.Timer(5.0, self._distribute_dataset_configurations, args=[federation_id]).start()
                        else:
                            self.logger.error("Failed to initialize federation")
                    else:
                        self.logger.info(f"This ALM ({self.actor_id}) is not the leader. Waiting for federation initialization.")
                else:
                    if num_providers < 2:
                        self.logger.info(f"Not enough providers registered ({num_providers}/2 needed). Waiting for more registrations.")
                    elif self.active_federations:
                        self.logger.info("Active federation already exists, skipping initialization")
                
            except Exception as e:
                self.logger.error(f"Error during provider registration processing: {e}", exc_info=True)
        else:
            self.logger.warning(f"Redis client not available, provider {provider_id} registration not persisted")
        
        return True

    def request_federation(self, context_id, parameters):
        """
        Sends a request to initialize a new federation.
        
        Args:
            context_id: A unique identifier for the federation context
            parameters: Federation parameters including dataset configuration
            
        Returns:
            federation_id: ID of the created federation or None if failed
        """
        try:
            # Generate a unique federation ID
            federation_id = f"fed_{uuid.uuid4().hex[:8]}"
            
            # Get all available providers from Redis
            if self.redis_client:
                provider_keys = self.redis_client.keys("provider:*")
                available_participants = []
                
                for key in provider_keys:
                    provider_id = key.decode('utf-8').split(':', 1)[1] if isinstance(key, bytes) else key.split(':', 1)[1]
                    available_participants.append(provider_id)
                
                # Create federation initialization message
                federation_message = {
                    'action': 'federation_init',
                    'federationID': federation_id,  # Important: use federationID for compatibility
                    'context_id': context_id,
                    'parameters': parameters,
                    'selectedParticipants': available_participants,
                    'initiator': self.actor_id,
                    'timestamp': time.time()
                }
                
                self.logger.info(f"Initiating federation {federation_id} with participants: {available_participants}")
                
                # Send the federation initialization message
                self.producer.produce(
                    self.federation_init_topic,
                    value=json.dumps(federation_message).encode('utf-8')
                )
                self.producer.flush()
                
                # Store local federation state
                self.active_federations[federation_id] = {
                    'status': 'initiated',
                    'parameters': parameters,
                    'participants': available_participants,
                    'start_time': time.time(),
                }
                
                return federation_id
                
            else:
                self.logger.error("Redis client not available, cannot retrieve provider list")
                return None
                
        except Exception as e:
            self.logger.error(f"Error initiating federation: {e}", exc_info=True)
            return None

    def _distribute_dataset_configurations(self, federation_id):
        """
        Envoie les configurations de dataset aux participants de la fédération
        
        Args:
            federation_id: Identifiant de la fédération active
        """
        if federation_id not in self.active_federations:
            self.logger.warning(f"Cannot distribute dataset configs: Federation {federation_id} not found in active federations")
            return
        
        federation = self.active_federations[federation_id]
        
        try:
            # Obtenir les informations de configuration du dataset depuis les paramètres
            dataset_config = federation['parameters'].get('dataset_config', {})
            
            if not dataset_config:
                self.logger.warning(f"No dataset configuration found for federation {federation_id}")
                return
                
            # Créer le message de configuration du dataset
            dataset_message = {
                'action': 'dataset_config',
                'federation_id': federation_id,
                'dataset_config': dataset_config,
                'timestamp': time.time()
            }
            
            self.logger.info(f"Distributing dataset configuration for federation {federation_id}")
            self.logger.debug(f"Dataset configuration: {dataset_config}")
            
            # Envoyer le message via le topic approprié
            self.producer.produce(
                self.federation_init_topic,  # Utiliser le topic principal pour la diffusion
                value=json.dumps(dataset_message).encode('utf-8')
            )
            self.producer.flush()
            
            self.logger.info(f"Dataset configuration distributed for federation {federation_id}")
            
            # Mettre à jour l'état de la fédération
            federation['status'] = 'dataset_configured'
            
        except Exception as e:
            self.logger.error(f"Error distributing dataset configuration: {e}", exc_info=True)

if __name__ == '__main__':
    # Use environment variable or default to the Docker service names
    kafka_servers = os.getenv("KAFKA_BROKERS", "kafka-1:19092,kafka-2:19093,kafka-3:19094")
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("ALM_MAIN")
    logger.info(f"Starting ALM with Kafka servers: {kafka_servers}")
    
    # Check if data directory exists, create if not
    data_dir = "/data"
    config_file = os.path.join(data_dir, "config.yml")

    if not os.path.exists(data_dir): 
        os.makedirs(data_dir)
        
    # Initialize and start the ALM actor
    try:
        alm = ALMActor(kafka_servers)
        logger.info("ALM actor initialized successfully")
        alm.start_listening()
    except Exception as e:
        logger.error(f"Failed to start ALM actor: {e}", exc_info=True)