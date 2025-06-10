import json
import time
import uuid
import logging
import threading
import yaml
import base64
import os
import socket
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import redis
import random
# Assuming these custom modules are in the Python path or same directory
from commun.logactor import LoggerActor
from commun.FedFcaMonitor import FedFcaMonitor
from commun.ConceptStabilityAnalyzer import ConceptStabilityAnalyzer
from commun.CryptoManager import CryptoManager # For federation-level/channel security
from commun.FederationManager import FederationManager # Manages federation lifecycle
from fedfca_core import FedFCA_core as core
from commun.RedisKMS import RedisKMS

class AGMActor:
    """
    Actor for Aggregation and Global Manager (AGM) in FedFCA.
    Implements the Autonomic Global Manager role from the sequence diagram.
    """

    def __init__(self, kafka_servers=None, max_workers=10):
        # Initialize logger first
        self.actor_id = f"AGM_{uuid.uuid4().hex[:8]}"
        logging.basicConfig(level=logging.INFO, format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(self.actor_id)
        
        # Initialize metrics logging
        self.logactor = LoggerActor(self.actor_id)
        
        # Then load config
        self.config_file_path = "/data/config.yml"
        self.config = self._load_config()
        self.logger.info(f"Config: {self.config}")
        # Use Kafka servers from config if not provided
        self.kafka_servers = kafka_servers or self.config.get("kafka", {}).get("bootstrap_servers", "kafka-1:19092,kafka-2:19093,kafka-3:19094")
        self.data_dir = self.config.get("data", {}).get("data_dir", "/data/federated_datasets")

        # Configure Redis connection
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
        self.aggregator = core.Aggregator(
            threshold=self.config.get("aggregator", {}).get("threshold", 0.05)
        )
        
        if hasattr(self.aggregator, 'set_encryption_service'):
            self.aggregator.set_encryption_service(self.data_encryption_service)

        self.quality_analyzer = ConceptStabilityAnalyzer()
        self.monitor = FedFcaMonitor()
        # Use the same key prefix as AGM to ensure keys are found
        redis_kms = RedisKMS(
            host=redis_host,
            port=redis_port,
            db=0,
            key_prefix="fedfca:kms:agm:",  # Match AGM's prefix
            password=redis_password,
            username=redis_user
        )
        
        # Initialize core components with RedisKMS
        self.crypto_manager = CryptoManager(kms=redis_kms)# For federation-level/channel security
        
        # Federation state management
        self.active_federations = {}  # {fed_id: {'participants': {}, 'status': '', 'round': 0, ...}}
        self.federation_manager = FederationManager(agm_actor=self)

        # Configure Kafka producer
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'message.max.bytes': 10485880,
            'linger.ms': 10,
            'retries': 3,
            'retry.backoff.ms': 1000,
            'client.id': f"producer_{self.actor_id}",
            'socket.timeout.ms': 10000,
            'socket.keepalive.enable': True,
            'reconnect.backoff.ms': 1000,
            'reconnect.backoff.max.ms': 5000,
            'security.protocol': 'PLAINTEXT'
        })
        
        # Kafka topics setup following the sequence diagram
        self.federation_init_topic = "federation.init"
        self.federation_response_topic = "federation.response"
        self.negotiation_keys_topic = "negotiation.keys"
        self.key_acknowledgment_topic = "key.acknowledgment"
        self.participant_lattice_topic = "participant.lattice"
        self.shuffle_operation_topic = "shuffle.operation"
        self.results_distribution_topic = "results.distribution"
        self.results_acknowledgment_topic = "results.acknowledgment"
        
        # Subscribe to required topics
        self.subscribed_topics = [
            self.federation_init_topic,
            self.federation_response_topic,
            self.key_acknowledgment_topic,
            self.participant_lattice_topic,
            self.results_acknowledgment_topic
        ]
        
        # Configure Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': self.config.get("kafka", {}).get("consumer_group_id", "agm_consumer_group"),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'client.id': f"consumer_{self.actor_id}",
            'socket.timeout.ms': 10000,
            'socket.keepalive.enable': True,
            'reconnect.backoff.ms': 1000,
            'reconnect.backoff.max.ms': 5000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'security.protocol': 'PLAINTEXT'
        })
        
        self.consumer.subscribe(self.subscribed_topics)
        
        self.logger.info(f"AGMActor {self.actor_id} initialized. Listening on: {self.subscribed_topics}")
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Track participant information
        self.federation_participants = defaultdict(dict)  # {federation_id: {participant_id: participant_info}}
        self.participant_lattices = defaultdict(dict)    # {federation_id: {participant_id: lattice_payload}}
        self.federation_keys = defaultdict(dict)         # {federation_id: {'K1': key, 'K4': key}}
        self.participant_responses = defaultdict(dict)   # {federation_id: {participant_id: 'ACCEPT'|'DECLINE'}}

    def _load_config(self):
        """Load configuration from YAML file."""
        try:
            with open(self.config_file_path, 'r') as file:
                config_data = yaml.safe_load(file)
                self.logger.info(f"Configuration loaded from {self.config_file_path}")
                return config_data if config_data else {}
        except FileNotFoundError:
            self.logger.warning(f"Config file not found: {self.config_file_path}. Using empty config.")
        except Exception as e:
            self.logger.error(f"Error loading config from {self.config_file_path}: {e}")
        return {}

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
                    'retry.backoff.ms': 1000
                })
                
                # Ensure all required topics exist based on sequence diagram
                required_topics = [
                    self.federation_init_topic,
                    self.federation_response_topic,
                    self.negotiation_keys_topic,
                    self.key_acknowledgment_topic,
                    self.participant_lattice_topic,
                    self.shuffle_operation_topic,
                    self.results_distribution_topic,
                    self.results_acknowledgment_topic
                ]
                
                for topic in required_topics:
                    try:
                        admin_client.produce(
                            topic,
                            value=json.dumps({"action": "topic_init"}).encode('utf-8')
                        )
                        self.logger.info(f"Topic {topic} verified or created")
                    except Exception as e:
                        self.logger.warning(f"Error checking/creating topic {topic}: {e}")
                        if retry < max_retries - 1:
                            self.logger.info(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            continue
                
                admin_client.flush()
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to ensure topics exist (attempt {retry + 1}/{max_retries}): {e}")
                if retry < max_retries - 1:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Failed to ensure topics exist after all retries")
                    return False

    def request_federation(self, context_id, parameters):
        """
        Handle request for federation from initiator
        Implements the "Request Federation" step from the sequence diagram
        """
        self.logger.info(f"Received federation request with context_id: {context_id}")
        
        # Generate federation ID
        federation_id = f"fed_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        
        # Ensure topics exist before proceeding
        if not self._ensure_topics_exist():
            self.logger.error("Failed to ensure required topics exist")
            return None
            
        # Query participant registry for available participants
        available_participants = self._query_participant_registry()
        
        if not available_participants:
            self.logger.error("No participants available for federation")
            return None
            
        # Select participants based on criteria
        selected_participants = self._select_participants(available_participants, parameters)
        
        self.logger.info(f"Selected {len(selected_participants)} participants for federation {federation_id}")
        
        # Initialize federation tracking
        self.active_federations[federation_id] = {
            'created_at': time.time(),
            'context_id': context_id,
            'parameters': parameters,
            'participants': {},
            'selected_participants': selected_participants,
            'status': 'initializing',
            'round': 0
        }
        
        # Publish federation initialization event
        self._publish_federation_init(federation_id, selected_participants, parameters)
        
        return federation_id

    def _query_participant_registry(self):
        """Query Participant Registry (PR) for available participants"""
        self.logger.info("Querying participant registry for available participants")
        
        # In a real implementation, this would query an actual participant registry service
        # For simulation purposes, we'll use a mock list or check connected ALMs via Redis
        available_participants = []
        
        try:
            if self.redis_client:
                # Query Redis for registered ALMs
                participant_keys = self.redis_client.keys("participant:*")
                self.logger.info(f"Found {len(participant_keys)} participant keys in Redis")
                
                for key in participant_keys:
                    try:
                        participant_data = self.redis_client.hgetall(key)
                        self.logger.debug(f"Redis data for {key}: {participant_data}")
                        
                        if participant_data and participant_data.get("status") == "active":
                            participant_id = key.split(":", 1)[1]
                            try:
                                capabilities = json.loads(participant_data.get("capabilities", "{}"))
                            except json.JSONDecodeError:
                                self.logger.warning(f"Invalid capabilities JSON for {participant_id}: {participant_data.get('capabilities')}")
                                capabilities = {}
                                
                            available_participants.append({
                                "participant_id": participant_id,
                                "capabilities": capabilities,
                                "status": participant_data.get("status")
                            })
                            self.logger.info(f"Added participant {participant_id} with capabilities: {capabilities}")
                    except Exception as e:
                        self.logger.error(f"Error processing participant data for {key}: {e}")
            
            # If no participants found in Redis or Redis not available, use mock data
            if not available_participants:
                self.logger.warning("No participants found in Redis, using mock data")
                # Mock data - in real implementation, would come from PR service
                for i in range(1, 6):  # Simulate 5 participants
                    available_participants.append({
                        "participant_id": f"ALM_{i}",
                        "capabilities": {"datasize": 1000 * i, "max_lattice_size": 5000},
                        "status": "active"
                    })
                
            self.logger.info(f"Found {len(available_participants)} available participants")
            return available_participants
            
        except Exception as e:
            self.logger.error(f"Error querying participant registry: {e}")
            # Return a default list in case of errors
            return [{"participant_id": f"ALM_{i}", "status": "active"} for i in range(1, 4)]
    
    def _select_participants(self, available_participants, parameters):
        """Select participants from available list based on criteria"""
        self.logger.info(f"Selecting participants based on criteria from {len(available_participants)} available")
        
        # Extract selection criteria from parameters
        min_participants = parameters.get("min_participants", 2)
        required_capabilities = parameters.get("required_capabilities", {})
        
        # Filter based on criteria
        qualified_participants = []
        for participant in available_participants:
            if participant.get("status") == "active":
                # Check capabilities if specified
                if required_capabilities:
                    participant_capabilities = participant.get("capabilities", {})
                    meets_requirements = True
                    for cap_name, cap_min_value in required_capabilities.items():
                        if cap_name not in participant_capabilities or participant_capabilities[cap_name] < cap_min_value:
                            meets_requirements = False
                            break
                    
                    if meets_requirements:
                        qualified_participants.append(participant["participant_id"])
                else:
                    qualified_participants.append(participant["participant_id"])
        
        # Check if we have enough qualified participants
        if len(qualified_participants) < min_participants:
            self.logger.warning(f"Not enough qualified participants. Required: {min_participants}, Found: {len(qualified_participants)}")
            # Take as many as we have
            return qualified_participants
        
        # If we have more participants than required, we could implement a selection strategy
        # For now, just return all qualified participants
        return qualified_participants
    
    def _publish_federation_init(self, federation_id, selected_participants, parameters):
        """Publish federation initialization event to Kafka"""
        self.logger.info(f"Publishing federation initialization for {federation_id} with {len(selected_participants)} participants")
        
        init_message = {
            'action': 'federation_init',
            'federationID': federation_id,
            'selectedParticipants': selected_participants,
            'parameters': parameters,
            'timestamp': time.time()
        }
        
        try:
            self.producer.produce(
                self.federation_init_topic,
                value=json.dumps(init_message).encode('utf-8')
            )
            self.producer.flush()
            self.logger.info(f"Published federation initialization to {self.federation_init_topic}")
            self.logger.debug(f"Federation initialization message: {init_message}")
            
            # Set up timeout for participant responses
            # In a real implementation, we'd use a more robust method like a scheduler
            threading.Timer(30.0, self._finalize_participant_list, args=[federation_id]).start()

        except Exception as e:
            self.logger.error(f"Failed to publish federation initialization: {e}")
    
    def _process_federation_response(self, message_data):
        """Process federation response from ALM - Event 1"""
        federation_id = message_data.get("federation_id")
        participant_id = message_data.get("participant_id")
        status = message_data.get("status")  # ACCEPT or DECLINE
        
        if not all([federation_id, participant_id, status]):
            self.logger.warning("Invalid federation response: Missing required fields")
            self.logger.debug(f"Received message data: {message_data}")
            return
        
        self.logger.info(f"Received federation response from {participant_id} for {federation_id}: {status}")
        
        if federation_id in self.active_federations:
            # Record participant response
            self.participant_responses[federation_id][participant_id] = status
            
            if status == "ACCEPT":
                # Add to participants list in federation
                self.active_federations[federation_id]['participants'][participant_id] = {
                    'status': 'joined',
                    'joined_at': time.time()
                }
                self.logger.info(f"Participant {participant_id} accepted federation {federation_id}")
            else:
                self.logger.info(f"Participant {participant_id} declined federation {federation_id}")
                
            # Check if we have responses from all selected participants
            self._check_all_responses_received(federation_id)
        else:
            self.logger.warning(f"Received response for unknown federation {federation_id}")
    
    def _check_all_responses_received(self, federation_id):
        """Check if we have received responses from all selected participants"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        selected_participants = set(federation['selected_participants'])
        responded_participants = set(self.participant_responses[federation_id].keys())
        
        # If all participants have responded, finalize the list
        if selected_participants.issubset(responded_participants):
            self.logger.info(f"All selected participants have responded for federation {federation_id}")
            self._finalize_participant_list(federation_id)
    
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
        else:
            self.logger.warning(f"Not enough participants accepted. Federation {federation_id} cancelled.")
            federation['status'] = 'cancelled'
            # In a real implementation, we would notify the initiator here
    
    def _start_key_negotiation(self, federation_id):
        """Start key negotiation protocol - Event 2"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        federation['status'] = 'key_negotiation'
        
        # Generate conference key K1
        k1_key = self.crypto_manager.generate_symmetric_key()
        self.federation_keys[federation_id]['K1'] = k1_key
        
        # Prepare key parameters
        key_parameters = {
            'key_type': 'AES-256',
            'key_id': f"k1_{federation_id}",
            'timestamp': time.time()
        }
        
        # Publish key negotiation message
        key_message = {
            'federationID': federation_id,
            'keyParameters': key_parameters,
            'timestamp': time.time()
        }
        
        try:
            self.producer.produce(
                self.negotiation_keys_topic,
                value=json.dumps(key_message).encode('utf-8')
            )
            self.producer.flush()
            self.logger.info(f"Published key negotiation parameters for federation {federation_id}")
            
            # Distribute key to each participant
            self._distribute_keys_to_participants(federation_id)
            
        except Exception as e:
            self.logger.error(f"Error during key negotiation for federation {federation_id}: {e}")
            federation['status'] = 'key_negotiation_failed'
    
    def _distribute_keys_to_participants(self, federation_id):
        """Distribute encryption keys to each participant"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        participants = federation['final_participants']
        k1_key = self.federation_keys[federation_id]['K1']
        
        # Track key acknowledgments
        federation['key_acks'] = set()
        
        # Distribute keys to each participant in parallel
        for participant_id in participants:
            try:
                # In a real implementation, we would encrypt K1 with the participant's public key
                # For simulation, we'll use a base64 encoded representation
                encrypted_k1 = base64.b64encode(k1_key).decode('utf-8')
                
                # Sign the message
                signature = self.crypto_manager.sign_message(encrypted_k1, private_key=None)
                
                # Send key distribution message
                key_dist_message = {
                    'action': 'key_distribution',
                    'federation_id': federation_id,
                    'encryptedK1': encrypted_k1,
                    'signatureAGM': signature,
                    'timestamp': time.time()
                }
                
                # Publish to participant-specific topic - use correct format
                topic = f"key.distribution.{participant_id}"
                self.logger.info(f"Sending key distribution to {participant_id} on topic {topic}")
                
                # Use delivery callback to track message delivery
                def delivery_callback(err, msg):
                    if err is not None:
                        self.logger.error(f"Key distribution to {participant_id} failed: {err}")
                    else:
                        self.logger.info(f"Key distribution message delivered to {participant_id}")
                
                self.producer.produce(
                    topic,
                    value=json.dumps(key_dist_message).encode('utf-8'),
                    on_delivery=delivery_callback
                )
                
            except Exception as e:
                self.logger.error(f"Error distributing key to {participant_id}: {e}")
        
        self.producer.flush(timeout=5)
    
    def _process_key_acknowledgment(self, message_data):
        """Process key acknowledgment from participant - Event 2"""
        federation_id = message_data.get("federation_id")
        participant_id = message_data.get("participant_id")
        key_receipt_id = message_data.get("key_receipt_id")
        signature = message_data.get("signature")
        
        if not all([federation_id, participant_id, key_receipt_id, signature]):
            self.logger.warning("Invalid key acknowledgment: Missing required fields")
            return
            
        if federation_id in self.active_federations:
            federation = self.active_federations[federation_id]
            
            # Verify signature
            is_valid = self.crypto_manager.verify_signature(key_receipt_id, signature)
            
            if is_valid:
                # Record acknowledgment
                if 'key_acks' not in federation:
                    federation['key_acks'] = set()
                federation['key_acks'].add(participant_id)
                
                self.logger.info(f"Participant {participant_id} acknowledged key receipt for federation {federation_id}")
                
                # Check if all participants have acknowledged
                if set(federation['final_participants']) == federation['key_acks']:
                    self.logger.info(f"All participants have acknowledged keys for federation {federation_id}")
                    federation['status'] = 'keys_distributed'
                    # Start dataset configuration distribution
                    self._distribute_dataset_configurations(federation_id)
                else:
                    self.logger.warning(f"Invalid signature in key acknowledgment from {participant_id}")
            else:
                self.logger.warning(f"Invalid signature in key acknowledgment from {participant_id}")
        else:
            self.logger.warning(f"Received key acknowledgment for unknown federation {federation_id}")
    
    def _distribute_dataset_configurations(self, federation_id):
        """Distribute dataset configurations to participants"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        participants = federation['final_participants']
        
        for participant_id in participants:
            try:
                # Prepare dataset configuration
                dataset_config = {
                    "threshold": 0.1,
                    "max_iterations": 100,
                    "convergence_threshold": 0.001,
                    "privacy_epsilon": 1.0,
                    "privacy_delta": 1e-5
                }
                
                # Sign the configuration
                signature = self.crypto_manager.sign_message(
                    json.dumps(dataset_config),
                    private_key=None
                )
                
                # Prepare message
                config_message = {
                    'action': 'dataset_config',
                    'federation_id': federation_id,
                    'dataset_config': dataset_config,
                    'signature': signature,
                    'timestamp': time.time()
                }
                
                # Generate a new encryption key for this federation
                try:
                    key_id = f"key_{federation_id}_{int(time.time())}"
                    self.logger.info(f"Generating encryption key for federation {federation_id}...")
                    
                    # Generate key with federation ID
                    key_id = self.crypto_manager.generate_key(
                        key_id=f"key_{federation_id}_{int(time.time())}",
                        key_type='fernet',
                        expiration=3600,  # 1 hour expiration
                        federation_id=federation_id  # Associate key with federation
                    )
                    
                    self.logger.info(f"Successfully generated and stored encryption key: {key_id} for federation {federation_id}")
                    
                    # Store the key ID in the federation config
                    federation_config['encryption_key'] = key_id
                    
                    # Verify the key was stored correctly
                    try:
                        key_data = self.crypto_manager.get_key(key_id)
                        if not key_data:
                            raise ValueError(f"Failed to retrieve key {key_id} after generation")
                        if 'federation_id' in key_data and key_data['federation_id'] != federation_id:
                            self.logger.warning(f"Key federation ID mismatch: expected {federation_id}, got {key_data.get('federation_id')}")
                        self.logger.debug(f"Verified key {key_id} is properly stored and accessible")
                    except Exception as e:
                        self.logger.error(f"Failed to verify key {key_id}: {e}", exc_info=True)
                        raise
                
                except Exception as e:
                    self.logger.error(f"Error generating encryption key for federation {federation_id}: {e}")
                    raise
                
                # Send to participant's topic
                topic = f"alm_{participant_id}"
                self.producer.produce(
                    topic,
                    value=json.dumps(config_message).encode('utf-8')
                )
                
                self.logger.info(f"Sent dataset configuration to {participant_id}")
                
            except Exception as e:
                self.logger.error(f"Error sending dataset configuration to {participant_id}: {e}")
        
        self.producer.flush()
        federation['status'] = 'config_distributed'
    
    def _process_participant_lattice(self, message_data):
        """Process lattice submission from participant - Event 3"""
        federation_id = message_data.get("federation_id")
        participant_id = message_data.get("participant_id")
        encrypted_lattice = message_data.get("encrypted_lattice")
        lattice_metadata = message_data.get("lattice_metadata")
        signature = message_data.get("signature")
        
        if not all([federation_id, participant_id, encrypted_lattice, signature]):
            self.logger.warning("Invalid lattice submission: Missing required fields")
            return
            
        if federation_id not in self.active_federations:
            self.logger.warning(f"Received lattice for unknown federation {federation_id}")
            return
            
        federation = self.active_federations[federation_id]
        
        try:
            # Verify signature
            is_valid = self.crypto_manager.verify_signature(encrypted_lattice, signature)
            
            if not is_valid:
                self.logger.warning(f"Invalid signature in lattice submission from {participant_id}")
                return
            
            # Store the encrypted lattice
            if 'lattices' not in federation:
                federation['lattices'] = {}
            
            federation['lattices'][participant_id] = {
                'encrypted_lattice': encrypted_lattice,
                'metadata': lattice_metadata,
                'received_at': time.time()
            }
            
            self.logger.info(f"Received encrypted lattice from {participant_id} for federation {federation_id}")
            
            # Check if all lattices have been received
            expected_participants = set(federation['final_participants'])
            received_participants = set(federation['lattices'].keys())
            
            if expected_participants == received_participants:
                self.logger.info(f"All lattices received for federation {federation_id}")
                federation['status'] = 'lattices_received'
                
                # Log metrics
                metrics = {
                    'total_participants': len(expected_participants),
                    'received_lattices': len(received_participants),
                    'completion_time': time.time() - federation['created_at']
                }
                self.logactor.log_metrics(metrics)
                
                # Proceed to apply differential privacy and shuffle
                self._apply_differential_privacy_and_shuffle(federation_id)
            else:
                self.logger.info(f"Waiting for more lattices. Received {len(received_participants)}/{len(expected_participants)}")
                
        except Exception as e:
            self.logger.error(f"Error processing lattice submission: {e}", exc_info=True)
            federation['status'] = 'lattice_processing_failed'
    
    def _apply_differential_privacy_and_shuffle(self, federation_id):
        """Apply differential privacy and prepare for shuffle - Event 4"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        
        try:
            self.logger.info(f"Applying differential privacy and preparing shuffle for federation {federation_id}")
            
            # Apply differential privacy noise
            epsilon = federation['parameters'].get('differential_privacy', {}).get('epsilon', 1.0)
            delta = federation['parameters'].get('differential_privacy', {}).get('delta', 0.0001)
            
            # Apply noise to each lattice
            noisy_lattices = []
            for participant_id, lattice_data in federation['lattices'].items():
                # In a real implementation, apply differential privacy noise
                noisy_lattices.append(lattice_data['encrypted_lattice'])
            
            # Remove source identifiers and randomize lattice order
            shuffled_lattices = []
            for lattice in noisy_lattices:
                # Remove any participant-specific identifiers
                shuffled_lattices.append(lattice)
            
            # Shuffle the lattice order
            random.shuffle(shuffled_lattices)
            
            # Generate K4 for secure transmission to Global Server
            k4_key = self.crypto_manager.generate_symmetric_key()
            self.federation_keys[federation_id]['K4'] = k4_key
            encrypted_k4 = base64.b64encode(k4_key).decode('utf-8')
            
            # Sign the shuffled batch
            signature = self.crypto_manager.sign_message(str(shuffled_lattices))
            
            # Publish shuffle operation
            shuffle_message = {
                'federation_id': federation_id,
                'shuffled_lattices': shuffled_lattices,
                'encrypted_k4': encrypted_k4,
                'signature': signature,
                'timestamp': time.time()
            }
            
            self.producer.produce(
                self.shuffle_operation_topic,
                value=json.dumps(shuffle_message).encode('utf-8')
            )
            self.producer.flush()
            
            federation['status'] = 'shuffle_completed'
            federation['shuffled_lattices'] = shuffled_lattices
            
            self.logger.info(f"Published shuffle operation for federation {federation_id}")
            
        except Exception as e:
            self.logger.error(f"Error during differential privacy and shuffle for federation {federation_id}: {e}")
            federation['status'] = 'shuffle_failed'
    
    def _process_results_acknowledgment(self, message_data):
        """Process results acknowledgment from participant - Event 6"""
        federation_id = message_data.get("federation_id")
        participant_id = message_data.get("participant_id")
        result_receipt_id = message_data.get("result_receipt_id")
        signature = message_data.get("signature")
        
        if not all([federation_id, participant_id, result_receipt_id, signature]):
            self.logger.warning("Invalid results acknowledgment: Missing required fields")
            return
            
        if federation_id in self.active_federations:
            federation = self.active_federations[federation_id]
            
            # Verify signature
            is_valid = self.crypto_manager.verify_signature(result_receipt_id, signature)
            
            if is_valid:
                # Record acknowledgment
                if 'result_acks' not in federation:
                    federation['result_acks'] = set()
                federation['result_acks'].add(participant_id)
                
                self.logger.info(f"Participant {participant_id} acknowledged results for federation {federation_id}")
                
                # Check if all participants have acknowledged
                if set(federation['final_participants']) == federation['result_acks']:
                    self.logger.info(f"All participants have acknowledged results for federation {federation_id}")
                    federation['status'] = 'completed'
                    
                    # Notify initiator of federation completion
                    self._notify_federation_completion(federation_id)
            else:
                self.logger.warning(f"Invalid signature in results acknowledgment from {participant_id}")
        else:
            self.logger.warning(f"Received results acknowledgment for unknown federation {federation_id}")
    
    def _notify_federation_completion(self, federation_id):
        """Notify the initiator that federation is complete"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        
        self.logger.info(f"Federation {federation_id} completed successfully")
        
        # In a real implementation, this would notify the initiator via appropriate channel
        # For simulation, we just log it
        completion_summary = {
            'federation_id': federation_id,
            'status': 'completed',
            'participants_count': len(federation['final_participants']),
            'duration_seconds': time.time() - federation['created_at']
        }
        
        self.logger.info(f"Federation completion summary: {completion_summary}")
        
        # In a real implementation, there would be a callback mechanism to the initiator

    def _process_incoming_message(self, message_data, topic):
        """Process incoming Kafka messages based on topic"""
        try:
            self.logger.info(f"Processing message on topic {topic}: {message_data.get('action', 'NO_ACTION')}")
            self.logger.debug(f"Full message content: {message_data}")
            
            # Ignorer les messages de vérification des topics
            if message_data.get("action") in ["topic_init", "topic_verify"]:
                self.logger.debug(f"Ignoring topic verification message on {topic}")
                return True

            # Traiter les messages d'enregistrement de fournisseur
            if topic == self.federation_init_topic and message_data.get("action") == "provider_register":
                self.logger.info(f"Detected provider registration message from {message_data.get('provider_id')}")
                result = self._process_provider_registration(message_data)
                self.logger.info(f"Provider registration processing result: {result}")
                return result
                
            # Traiter les messages federation_init spécifiquement
            if topic == self.federation_init_topic and message_data.get("action") == "federation_init":
                federation_id = message_data.get("federationID") or message_data.get("federation_id")
                self.logger.info(f"Received federation initialization request: {federation_id}")
                
                if not federation_id:
                    self.logger.warning("Invalid federation initialization request: Missing federation ID")
                    return True
                
                selected_participants = message_data.get("selectedParticipants", [])
                parameters = message_data.get("parameters", {})
                
                # Republier le message aux participants sélectionnés
                self._publish_federation_init(federation_id, selected_participants, parameters)
                self.logger.info(f"Federation initialization message forwarded to {len(selected_participants)} participants")
                return True
                
            # Traiter les messages dataset_config spécifiquement
            if topic == self.federation_init_topic and message_data.get("action") == "dataset_config":
                federation_id = message_data.get("federation_id")
                self.logger.info(f"Received dataset configuration for federation: {federation_id}")
                
                if not federation_id:
                    self.logger.warning("Invalid dataset configuration: Missing federation ID")
                    return True
                
                # Rediriger la configuration du dataset vers les participants appropriés
                # Vous pouvez directement rediriger ou traiter davantage si nécessaire
                if federation_id in self.active_federations:
                    participants = self.active_federations[federation_id].get('final_participants', [])
                    self.logger.info(f"Forwarding dataset configuration to {len(participants)} participants")
                    
                    for participant_id in participants:
                        try:
                            topic = f"alm_{participant_id}"
                            self.producer.produce(
                                topic,
                                value=json.dumps(message_data).encode('utf-8')
                            )
                            self.logger.info(f"Forwarded dataset configuration to {participant_id}")
                        except Exception as e:
                            self.logger.error(f"Error forwarding dataset configuration to {participant_id}: {e}")
                    
                    self.producer.flush()
                else:
                    self.logger.warning(f"Received dataset configuration for unknown federation: {federation_id}")
                
                return True

            action = message_data.get("action")
            self.logger.info(f"AGM received action: {action} on topic: {topic}")

            if topic == self.federation_response_topic:
                # Event 1: Federation Response
                if not all([message_data.get("federation_id"), message_data.get("participant_id"), message_data.get("status")]):
                    self.logger.warning(f"Skipping incomplete federation response message: {message_data}")
                    return True
                    
                self.logger.info(f"Processing federation response from {message_data.get('participant_id')}")
                self._process_federation_response(message_data)
                self.logger.info(f"Federation response processing completed for {message_data.get('participant_id')}")
                
            elif topic == self.key_acknowledgment_topic:
                # Event 2: Key Negotiation Protocol
                if not all([message_data.get("federation_id"), message_data.get("participant_id"), 
                          message_data.get("key_receipt_id"), message_data.get("signature")]):
                    self.logger.warning(f"Skipping incomplete key acknowledgment message: {message_data}")
                    return True
                    
                self.logger.info(f"Processing key acknowledgment from {message_data.get('participant_id')}")
                self._process_key_acknowledgment(message_data)
                self.logger.info(f"Key acknowledgment processing completed for {message_data.get('participant_id')}")
                
            elif topic == self.participant_lattice_topic:
                # Event 3: Lattice Submission
                if not all([message_data.get("federation_id"), message_data.get("participant_id"),
                          message_data.get("encrypted_lattice"), message_data.get("signature")]):
                    self.logger.warning(f"Skipping incomplete lattice submission message: {message_data}")
                    return True
                    
                self.logger.info(f"Processing lattice submission from {message_data.get('participant_id')}")
                self._process_participant_lattice(message_data)
                self.logger.info(f"Lattice submission processing completed for {message_data.get('participant_id')}")
                
            elif topic == self.results_acknowledgment_topic:
                # Event 6: Results Distribution
                if not all([message_data.get("federation_id"), message_data.get("participant_id"),
                          message_data.get("result_receipt_id"), message_data.get("signature")]):
                    self.logger.warning(f"Skipping incomplete results acknowledgment message: {message_data}")
                    return True
                    
                self.logger.info(f"Processing results acknowledgment from {message_data.get('participant_id')}")
                self._process_results_acknowledgment(message_data)
                self.logger.info(f"Results acknowledgment processing completed for {message_data.get('participant_id')}")
                
            else:
                self.logger.warning(f"Unknown topic: {topic}, message cannot be processed")
                return True  # Changé de False à True pour éviter les arrêts inattendus
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing message on topic {topic}: {e}", exc_info=True)
            self.logger.error(f"Message that caused the error: {message_data}")
            return True  # Changé de False à True pour éviter les arrêts inattendus

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
                # Use hset instead of hmset
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
                
                # Verify data was stored correctly
                stored_data = self.redis_client.hgetall(provider_key)
                self.logger.debug(f"Verification of stored data: {stored_data}")
            except Exception as e:
                self.logger.error(f"Error saving provider {provider_id} to Redis: {e}", exc_info=True)
        else:
            self.logger.warning(f"Redis client not available, provider {provider_id} registration not persisted")
        
        return True

    def start_listening(self):
        """Starts the main Kafka message listening loop for AGM."""
        self.logger.info(f"AGM {self.actor_id} actively listening...")
        
        # Ensure topics exist
        self._ensure_topics_exist()

        try:
            self.logger.info("Entering main message polling loop")
            while True:
                try:
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None: 
                        continue
                        
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF: 
                            continue
                        elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            self.logger.warning(f"Topic {msg.topic()} doesn't exist yet. Waiting...")
                            time.sleep(2)
                            continue
                        else:
                            self.logger.error(f"Kafka consume error: {msg.error()}")
                            # Try to reconnect
                            try:
                                self.logger.info("Attempting to reconnect to Kafka")
                                self.consumer.close()
                                self.consumer = Consumer({
                                    'bootstrap.servers': self.kafka_servers,
                                    'group.id': self.config.get("kafka", {}).get("consumer_group_id", "agm_consumer_group"),
                                    'auto.offset.reset': 'earliest',
                                    'enable.auto.commit': True,
                                    'client.id': f"consumer_{self.actor_id}",
                                    'socket.timeout.ms': 10000,
                                    'socket.keepalive.enable': True,
                                    'reconnect.backoff.ms': 1000,
                                    'reconnect.backoff.max.ms': 5000,
                                    'session.timeout.ms': 30000,
                                    'heartbeat.interval.ms': 10000,
                                    'security.protocol': 'PLAINTEXT'
                                })
                                self.consumer.subscribe(self.subscribed_topics)
                                self.logger.info(f"Reconnected to topics: {self.subscribed_topics}")
                                continue
                            except Exception as reconnect_err:
                                self.logger.error(f"Reconnection failed: {reconnect_err}")
                                break
                    
                    try:
                        # Check if message is empty
                        if not msg.value() or len(msg.value()) == 0:
                            self.logger.debug(f"Empty message received on topic {msg.topic()}, ignored")
                            continue
                        
                        self.logger.debug(f"Received message on topic {msg.topic()}, parsing content")
                        message_data = json.loads(msg.value().decode('utf-8'))
                        
                        # Process the message
                        if not self._process_incoming_message(message_data, msg.topic()):
                            self.logger.warning(f"Message processing returned False, breaking loop")
                            break
                            
                    except json.JSONDecodeError:
                        self.logger.warning(f"Non-JSON message ignored on topic {msg.topic()}: {msg.value()}")
                        continue
                        
                except Exception as processing_error:
                    self.logger.error(f"Error in message processing loop: {processing_error}", exc_info=True)
        
        except KeyboardInterrupt:
            self.logger.info("AGM listening loop interrupted.")
        finally:
            self.stop_actor()

    def stop_actor(self):
        """Gracefully stops the AGM actor."""
        self.logger.info("AGM stopping...")
        
        # Stop any monitoring in progress
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
            self.logger.info(f"AGM Final Monitoring Data: {self.monitor.get_metrics()}")
        except Exception as e:
            self.logger.warning(f"Could not get monitoring metrics: {e}")
        
        # Clean up any active federations
        if hasattr(self, 'active_federations'):
            for fed_id in self.active_federations:
                self.logger.info(f"Cleaning up federation {fed_id}")
                # Notify participants of shutdown
                self._notify_federation_shutdown(fed_id)
        
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
                "action": "agm_shutdown",
                "actor_id": self.actor_id,
                "timestamp": time.time()
            }
            if hasattr(self.logactor, 'log_metrics'):
                self.logactor.log_metrics(shutdown_metrics)
            else:
                self.logger.info(f"Shutdown metrics: {shutdown_metrics}")
        except Exception as e:
            self.logger.warning(f"Could not log shutdown metrics: {e}")
            
        self.logger.info(f"AGM {self.actor_id} has stopped.")

    def _notify_federation_shutdown(self, federation_id):
        """Notify all participants that the federation is shutting down"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        
        shutdown_message = {
            'action': 'federation_shutdown',
            'federation_id': federation_id,
            'reason': 'agm_shutdown',
            'timestamp': time.time()
        }
        
        for participant_id in federation.get('final_participants', []):
            try:
                topic = f"alm_{participant_id}"
                self.producer.produce(
                    topic,
                    value=json.dumps(shutdown_message).encode('utf-8')
                )
                self.logger.info(f"Sent shutdown notification to {participant_id}")
            except Exception as e:
                self.logger.error(f"Error sending shutdown notification to {participant_id}: {e}")
        
        self.producer.flush()

    def _handle_federation_timeout(self, federation_id):
        """Handle timeout for federation operations"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        
        # Check current status and handle accordingly
        if federation['status'] == 'initializing':
            # Notify participants that federation is cancelled
            self._notify_federation_cancelled(federation_id, "timeout_during_initialization")
        elif federation['status'] == 'key_negotiation':
            # Retry key negotiation
            self._start_key_negotiation(federation_id)
        elif federation['status'] == 'lattices_received':
            # Proceed with differential privacy and shuffle
            self._apply_differential_privacy_and_shuffle(federation_id)
        else:
            self.logger.warning(f"Unexpected timeout for federation {federation_id} in status {federation['status']}")

    def _notify_federation_cancelled(self, federation_id, reason):
        """Notify participants that federation is cancelled"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        
        cancel_message = {
            'action': 'federation_cancelled',
            'federation_id': federation_id,
            'reason': reason,
            'timestamp': time.time()
        }
        
        for participant_id in federation.get('final_participants', []):
            try:
                topic = f"alm_{participant_id}"
                self.producer.produce(
                    topic,
                    value=json.dumps(cancel_message).encode('utf-8')
                )
                self.logger.info(f"Sent cancellation notification to {participant_id}")
            except Exception as e:
                self.logger.error(f"Error sending cancellation notification to {participant_id}: {e}")
        
        self.producer.flush()
        federation['status'] = 'cancelled'

    def _handle_federation_error(self, federation_id, error_type, error_details):
        """Handle federation errors"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        
        error_message = {
            'action': 'federation_error',
            'federation_id': federation_id,
            'error_type': error_type,
            'error_details': error_details,
            'timestamp': time.time()
        }
        
        # Log the error
        self.logger.error(f"Federation {federation_id} error: {error_type} - {error_details}")
        
        # Notify participants
        for participant_id in federation.get('final_participants', []):
            try:
                topic = f"alm_{participant_id}"
                self.producer.produce(
                    topic,
                    value=json.dumps(error_message).encode('utf-8')
                )
            except Exception as e:
                self.logger.error(f"Error sending error notification to {participant_id}: {e}")
        
        self.producer.flush()
        
        # Update federation status
        federation['status'] = 'error'
        federation['error'] = {
            'type': error_type,
            'details': error_details,
            'timestamp': time.time()
        }

    def _cleanup_federation(self, federation_id):
        """Clean up federation resources"""
        if federation_id not in self.active_federations:
            return
            
        federation = self.active_federations[federation_id]
        
        # Clean up keys
        if federation_id in self.federation_keys:
            del self.federation_keys[federation_id]
        
        # Clean up lattices
        if federation_id in self.participant_lattices:
            del self.participant_lattices[federation_id]
        
        # Clean up responses
        if federation_id in self.participant_responses:
            del self.participant_responses[federation_id]
        
        # Remove from active federations
        del self.active_federations[federation_id]
        
        self.logger.info(f"Cleaned up resources for federation {federation_id}")

if __name__ == '__main__':
    # Use environment variable or default to the Docker service names
    kafka_bs = os.getenv("KAFKA_BROKERS", "kafka-1:19092,kafka-2:19093,kafka-3:19094") 

    data_dir = "/data"
    config_file = os.path.join(data_dir, "config.yml")

    if not os.path.exists(data_dir): 
        os.makedirs(data_dir)
        
    if not os.path.exists(config_file):
        default_agm_config = {
            "aggregator": {"threshold": 0.05},
            "kafka": {
                "bootstrap_servers": kafka_bs,
                "topics": {
                    "federation_init": "federation.init",
                    "federation_response": "federation.response",
                    "negotiation_keys": "negotiation.keys",
                    "key_acknowledgment": "key.acknowledgment",
                    "participant_lattice": "participant.lattice",
                    "shuffle_operation": "shuffle.operation",
                    "results_distribution": "results.distribution",
                    "results_acknowledgment": "results.acknowledgment"
                },
                "consumer_group_id": "agm_consumer_group"
            },
            "federation": {
                "timeout_seconds": 30,
                "min_participants": 2,
                "max_participants": 10,
                "differential_privacy": {
                    "epsilon": 1.0,
                    "delta": 0.0001
                }
            }
        }
        with open(config_file, "w") as f: 
            yaml.dump(default_agm_config, f)
        logging.info(f"Created default AGM config: {config_file}")

    agm = AGMActor(kafka_bs)
    agm.start_listening()
