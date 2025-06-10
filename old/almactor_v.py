import json
import time
import logging
import uuid as uuid
import os
import base64
import redis
from confluent_kafka import Producer, Consumer, KafkaError
from concurrent.futures import ThreadPoolExecutor
import threading

# Import core components
from fedfca_core import FedFCA_core as core
from commun.logactor import LoggerActor
from commun.FedFcaMonitor import FedFcaMonitor
from commun.CryptoManager import CryptoManager

class ALMActor:
    """
    Simplified Actor for Local Model (ALM) in FedFCA.
    Implements Redis-based registration and streamlined message exchange.
    """
    
    def __init__(self, kafka_servers=None, max_workers=10):
        # Initialize actor ID and logging
        self.actor_id = f"ALM_{os.environ.get('ACTOR_ID_SUFFIX', uuid.uuid4().hex[:6])}"
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s:%(name)s:%(message)s')
        self.logger = logging.getLogger(self.actor_id)
        
        # Initialize performance monitoring
        self.monitor = FedFcaMonitor()
        
        # Load configuration
        self.config_file_path = "/data/config.yml"
        self.config = self._load_config()
        
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
        
        # Initialize core components
        self.crypto_manager = CryptoManager()
        self.lattice_builder = core.FasterFCA(
            threshold=self.config.get("provider", {}).get("threshold", 0.5),
            context_sensitivity=self.config.get("provider", {}).get("context_sensitivity", 0.2)
        )
        
        # Use only the running Kafka broker
        self.kafka_servers = kafka_servers or os.environ.get("KAFKA_BROKERS", "kafka-1:9092")
        self.logger.info(f"Using Kafka brokers: {self.kafka_servers}")
        
        # Configure Kafka producer with enhanced settings
        producer_config = {
            'bootstrap.servers': self.kafka_servers,
            'message.max.bytes': 10485880,  # 10MB max message size
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.ms': 1000,
            'compression.type': 'gzip',
            'retries': 5,
            'retry.backoff.ms': 1000,
            'acks': 'all'  # Ensure messages are committed to all in-sync replicas
        }
        self.producer = Producer(producer_config)
        
        # Configure Kafka consumer with enhanced settings and detailed logging
        self.logger.info(f"Initializing Kafka consumer with bootstrap servers: {self.kafka_servers}")
        
        # Split the brokers and try each one
        brokers = self.kafka_servers.split(',')
        working_brokers = []
        
        # Test each broker
        for broker in brokers:
            test_config = {
                'bootstrap.servers': broker,
                'group.id': 'test_connection',
                # 'session.timeout.ms': 5000,
                # 'api.version.request': True,
                # 'api.version.fallback.ms': 0,
                # 'broker.version.fallback': '2.0.0'
            }
            
            try:
                test_consumer = Consumer(test_config)
                topics = test_consumer.list_topics(timeout=5.0)
                working_brokers.append(broker)
                self.logger.info(f"Successfully connected to broker: {broker}")
                test_consumer.close()
            except Exception as e:
                self.logger.warning(f"Failed to connect to broker {broker}: {str(e)}")
        
        if not working_brokers:
            raise Exception("Could not connect to any Kafka brokers")
            
        self.logger.info(f"Using working brokers: {', '.join(working_brokers)}")
                # Enhanced consumer configuration with improved reliability settings
        consumer_config = {
            'bootstrap.servers': ','.join(working_brokers),
            'group.id': f'alm_group_{self.actor_id}',  # Unique group ID per instance
            'client.id': f'alm_actor_{self.actor_id}',
            'auto.offset.reset': 'earliest',  # Start from beginning of topic if no offset is stored
            # 'enable.auto.commit': False,  # Manual commit after processing
            # 'enable.auto.offset.store': False,  # Manual offset storage
            # 'session.timeout.ms': 30000,  # 30 seconds session timeout
            # 'heartbeat.interval.ms': 10000,  # 10 seconds heartbeat interval
            # 'max.poll.interval.ms': 300000,  # 5 minutes max poll interval
            # 'fetch.min.bytes': 1,  # Send fetch request as soon as there is data
            # 'fetch.wait.max.ms': 500,  # Maximum time to wait for data
            # 'max.partition.fetch.bytes': 10485760,  # 10MB max per partition
            # 'log.connection.close': True,  # Log connection close events
            # 'debug': 'all',
            # 'socket.keepalive.enable': True,
            # 'socket.timeout.ms': 30000,  # 30 seconds socket timeout
            # 'socket.connection.setup.timeout.ms': 10000,  # 10 seconds connection setup timeout
            # 'api.version.request': True,
            # 'api.version.fallback.ms': 0,
            # 'broker.version.fallback': '2.0.0',
            # 'log.thread.name': True,
            # 'reconnect.backoff.ms': 1000,  # Initial backoff time
            # 'reconnect.backoff.max.ms': 60000  # Maximum backoff time
        }
        
        # Define topics to subscribe to
        self.subscribed_topics = [
            "provider.config",         # Configuration from AGM
            "federation.start",        # Federation start notification
            "federation.complete"      # Federation completion notification
        ]
        
        # Clean up any callback configurations that might be in the config
        for cb in ['error_cb', 'log_cb', 'rebalance_cb']:
            if cb in consumer_config:
                del consumer_config[cb]
        
        # Create and configure consumer
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.logger.info(f"Creating Kafka consumer (attempt {retry_count + 1}/{max_retries})...")
                self.consumer = Consumer(consumer_config)
                self.logger.info("Kafka consumer created successfully")
                
                # Test connection by listing topics
                try:
                    topics = self.consumer.list_topics(timeout=10.0)
                    
                    # Check if our topics exist
                    for topic in self.subscribed_topics:
                        if topic not in topics.topics:
                            self.logger.warning(f"Topic '{topic}' not found in available topics")
                    
                    # Subscribe to topics
                    self.consumer.subscribe(self.subscribed_topics)
                    
                    # Verify subscription was successful
                    msg = self.consumer.poll(1.0)
                    self.logger.info(f"Successfully subscribed to topics: {self.subscribed_topics}")
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    self.logger.error(f"Error during topic subscription: {e}")
                    retry_count += 1
                    if retry_count < max_retries:
                        self.logger.info(f"Retrying in 5 seconds... (attempt {retry_count + 1}/{max_retries})")
                        time.sleep(5)
                    else:
                        raise
                        
            except Exception as e:
                self.logger.error(f"Failed to create Kafka consumer: {e}", exc_info=True)
                retry_count += 1
                if retry_count >= max_retries:
                    self.logger.error("Max retries reached, giving up on Kafka consumer creation")
                    raise
                self.logger.info(f"Retrying in 5 seconds... (attempt {retry_count + 1}/{max_retries})")
                time.sleep(5)
        
        # Log consumer configuration (without sensitive data)
        safe_config = consumer_config.copy()
        if 'sasl.password' in safe_config:
            safe_config['sasl.password'] = '***'
        self.logger.info(f"Consumer configuration: {safe_config}")
        
        self.logger.info(f"ALMActor {self.actor_id} initialized")
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # State tracking
        self.active_federations = {}  # {fed_id: {'config': {}, 'status': ''}}
        self.federation_metrics = {}  # {fed_id: {'lattice_size': size, 'computation_time': time}}
        self.last_commit_time = time.time()  # Track last commit time for offset commits
        
        # Provider index (from environment)
        self.provider_index = int(os.environ.get('PROVIDER_INDEX', 0))
        
        # Track if consumer is healthy
        self.consumer_healthy = True
        
        # Register with Redis on initialization
        self.register_with_redis()
    
    def register_with_redis(self):
        """
        Register this provider in Redis for federation participation
        """
        try:
            if self.redis_client:
                registration_data = {
                    "provider_id": self.actor_id,
                    "status": "ready",
                    "capacity": self._estimate_data_capacity(),
                    "timestamp": time.time()
                }
                
                # Store in Redis hash 'federated_providers'
                self.redis_client.hset("federated_providers", self.actor_id, json.dumps(registration_data))
                self.logger.info(f"Provider {self.actor_id} registered in Redis")
                
                # Set TTL for auto-cleanup if provider goes offline
                self.redis_client.expire(f"provider:{self.actor_id}", 300)  # 5 minute TTL
                return True
            else:
                self.logger.error("Redis client not available, cannot register")
                return False
        except Exception as e:
            self.logger.error(f"Error registering provider: {e}")
            return False
    
    def _estimate_data_capacity(self):
        """
        Estimate the data processing capacity of this provider
        """
        # Simple capacity estimate - could be improved with actual metrics
        return 100  # Placeholder value
    
    def _load_config(self):
        """
        Load configuration from YAML file
        """
        import yaml
        try:
            if os.path.exists(self.config_file_path):
                with open(self.config_file_path, 'r') as file:
                    return yaml.safe_load(file)
            else:
                self.logger.warning(f"Config file not found at {self.config_file_path}, using defaults")
                return {}
        except Exception as e:
            self.logger.error(f"Error loading config: {e}")
            return {}
    
    def _process_incoming_message(self, message_data):
        """
        Process incoming message data from Kafka.
        
        Args:
            message_data: The message data to process. Can be a dict, string, or bytes.
            
        Returns:
            bool: True if message was processed successfully, False otherwise
        """
        if message_data is None:
            self.logger.error("Received None message data")
            return False
            
        # Log the type of message data received for debugging
        self.logger.debug(f"Processing message data of type: {type(message_data).__name__}")
        
        try:
            # If message_data is already a dict (e.g., from _process_kafka_message)
            if isinstance(message_data, dict):
                message = message_data
            # If it's a string, try to parse as JSON
            elif isinstance(message_data, str):
                try:
                    message = json.loads(message_data)
                    if not isinstance(message, dict):
                        self.logger.warning(f"Expected JSON object but got: {type(message).__name__}")
                        return False
                except json.JSONDecodeError as je:
                    self.logger.error(f"Failed to parse message as JSON: {je}")
                    return False
            # If it's bytes, decode then parse as JSON
            elif isinstance(message_data, bytes):
                try:
                    message_str = message_data.decode('utf-8')
                    try:
                        message = json.loads(message_str)
                        if not isinstance(message, dict):
                            self.logger.warning(f"Expected JSON object but got: {type(message).__name__}")
                            return False
                    except json.JSONDecodeError as je:
                        self.logger.error(f"Failed to parse decoded message as JSON: {je}")
                        return False
                except UnicodeDecodeError as ude:
                    self.logger.error(f"Failed to decode message as UTF-8: {ude}")
                    return False
            else:
                self.logger.error(f"Unsupported message type: {type(message_data).__name__}")
                return False
                
            # At this point, message should be a dict
            if not isinstance(message, dict):
                self.logger.error(f"Invalid message format after parsing: {type(message).__name__}")
                return False
            
            # Log the action if present
            action = message.get('action')
            if action:
                self.logger.info(f"Processing message with action: {action}")
            else:
                self.logger.warning("No action specified in message")
            
            # Extract topic from message or use default
            topic = message.get('topic')
            if not topic and hasattr(message_data, 'topic'):
                try:
                    topic = message_data.topic()
                except Exception:
                    pass
            
            # Log message details
            self.logger.debug(
                "Message details - Topic: %s, Message ID: %s, Action: %s",
                topic or 'unknown',
                message.get('message_id', 'N/A'),
                action or 'N/A'
            )
            
            # Log a sample of the message (truncated for large messages)
            msg_str = str(message)
            preview = msg_str[:500] + ('...' if len(msg_str) > 500 else '')
            self.logger.debug("Message content (first 500 chars): %s", preview)
            
            # Route message based on action or topic
            handler_map = {
                # Action-based handlers (from message.action)
                "provider_config": ("Processing provider configuration", self._handle_provider_config),
                "federation_start": ("Processing federation start message", self._handle_federation_start),
                "federation_complete": ("Processing federation completion", self._handle_federation_complete),
                "lattice_request": ("Processing lattice request", self._handle_lattice_request),
                "model_update": ("Processing model update", self._handle_model_update),
                "federation_status": ("Processing federation status update", self._handle_federation_status),
                
                # Topic-based fallbacks (from message.topic)
                "provider.config": ("Processing provider configuration (topic)", self._handle_provider_config),
                "federation.start": ("Processing federation start (topic)", self._handle_federation_start),
                "federation.complete": ("Processing federation completion (topic)", self._handle_federation_complete)
            }
            
            # Try to find a handler based on action first, then topic
            handler_key = action or topic
            if handler_key and handler_key in handler_map:
                log_msg, handler = handler_map[handler_key]
                self.logger.info(log_msg)
                try:
                    result = handler(message)
                    if result is False:
                        self.logger.warning(f"Handler for {handler_key} returned False")
                    return result
                except KeyError as ke:
                    self.logger.error(f"Missing required field in message: {ke}", exc_info=True)
                    return False
                except Exception as e:
                    self.logger.error(f"Error processing {handler_key} message: {e}", exc_info=True)
                    return False
            else:
                self.logger.warning(f"No handler found for action/topic: {handler_key}")
                # Try to handle it anyway with default handler if available
                if hasattr(self, '_handle_default'):
                    try:
                        self.logger.info("Attempting to process with default handler")
                        return self._handle_default(message)
                    except Exception as e:
                        self.logger.error(f"Error in default handler: {e}", exc_info=True)
                        return False
                return False
                
        except Exception as e:
            self.logger.error(f"Unexpected error in message processing: {e}", exc_info=True)
            return False
        
    def _handle_provider_config(self, message):
        """
        Handle configuration message from AGM
        Contains encryption keys and dataset configuration
        """
        try:
            self.logger.info(f"=== STARTING PROVIDER CONFIGURATION ===")
            self.logger.info(f"Received provider configuration: {json.dumps(message, indent=2, default=str)}")
            
            # Extract required fields
            federation_id = message.get('federation_id')
            provider_id = message.get('provider_id')
            encryption_key = message.get('encryption_key')
            dataset_config = message.get('dataset_config', {})
            
            # Validate required fields
            if not all([federation_id, provider_id, encryption_key, dataset_config]):
                missing = []
                if not federation_id: missing.append('federation_id')
                if not provider_id: missing.append('provider_id')
                if not encryption_key: missing.append('encryption_key')
                if not dataset_config: missing.append('dataset_config')
                self.logger.error(f"Missing required fields in provider config: {', '.join(missing)}")
                return False
            
            # Log the configuration
            self.logger.info(f"Processing configuration for federation: {federation_id}")
            self.logger.info(f"Provider ID: {provider_id}")
            self.logger.info(f"Dataset config: {json.dumps(dataset_config, indent=2)}")
            
            # Start performance monitoring
            start_time = time.time()
            self.monitor.start_timer(f"config_processing_{federation_id}")
            
            # Store federation configuration
            self.active_federations[federation_id] = {
                'status': 'configured',
                'config': dataset_config,
                'encryption_key': encryption_key,
                'start_time': start_time,
                'provider_id': provider_id
            }
            
            # Load dataset
            dataset_path = dataset_config.get('path')
            if not dataset_path:
                error_msg = f"No dataset path specified in configuration for federation {federation_id}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            
            self.logger.info(f"Loading dataset from {dataset_path}")
            self.monitor.start_timer(f"dataset_load_{federation_id}")
            try:
                dataset = self._load_dataset(dataset_path)
                dataset_load_time = self.monitor.stop_timer(f"dataset_load_{federation_id}")
                self.logger.info(f"Successfully loaded dataset with {len(dataset)} items")
            except Exception as e:
                error_msg = f"Failed to load dataset from {dataset_path}: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                raise RuntimeError(error_msg) from e
            
            # Compute local lattice
            self.logger.info(f"Computing local lattice for federation {federation_id}")
            self.monitor.start_timer(f"lattice_compute_{federation_id}")
            try:
                threshold = float(dataset_config.get('threshold', 0.5))
                self.logger.info(f"Using threshold: {threshold}")
                local_lattice = self._compute_local_lattice(dataset, threshold)
                lattice_compute_time = self.monitor.stop_timer(f"lattice_compute_{federation_id}")
                self.logger.info(f"Computed lattice with {len(local_lattice) if local_lattice else 0} concepts")
            except Exception as e:
                error_msg = f"Failed to compute lattice: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                raise RuntimeError(error_msg) from e
            
            # Set up encryption key
            key_id = f"fed_{federation_id}"
            try:
                if isinstance(encryption_key, str):
                    # If key is a string, it might be base64 encoded
                    try:
                        encryption_key = base64.b64decode(encryption_key)
                    except Exception:
                        # If not base64, encode as bytes
                        encryption_key = encryption_key.encode()
                
                # Set the key in the crypto manager
                self.crypto_manager.set_key(key_id, encryption_key)
                self.logger.info(f"Successfully set up encryption key for federation {federation_id}")
                
                # Encrypt lattice
                self.logger.info(f"Encrypting lattice for federation {federation_id}")
                self.monitor.start_timer(f"lattice_encrypt_{federation_id}")
                
                # Convert lattice to JSON string if it's not already
                if not isinstance(local_lattice, (str, bytes)):
                    local_lattice = json.dumps(local_lattice)
                
                encrypted_lattice = self.crypto_manager.encrypt(key_id, local_lattice)
                encryption_time = self.monitor.stop_timer(f"lattice_encrypt_{federation_id}")
                self.logger.info("Successfully encrypted lattice")
            except Exception as e:
                error_msg = f"Failed to set up encryption: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                raise RuntimeError(error_msg) from e
            
            # Prepare metrics
            lattice_size = len(json.dumps(encrypted_lattice))
            metrics = {
                'dataset_load_time': dataset_load_time,
                'lattice_compute_time': lattice_compute_time,
                'encryption_time': encryption_time,
                'lattice_size': lattice_size,
                'total_time': time.time() - start_time,
                'provider_id': provider_id,
                'federation_id': federation_id
            }
            
            # Store metrics
            self.federation_metrics[federation_id] = metrics
            
            # Prepare response
            response = {
                'action': 'lattice_result',
                'federation_id': federation_id,
                'provider_id': provider_id,
                'encrypted_lattice': encrypted_lattice,
                'metrics': metrics,
                'timestamp': time.time(),
                'message_id': f"lattice_{federation_id}_{provider_id}_{int(time.time())}"
            }
            
            # Send encrypted lattice to AGM
            self.logger.info(f"Sending lattice result for federation {federation_id}")
            self.monitor.start_timer(f"lattice_transmission_{federation_id}")
            
            try:
                self.producer.produce(
                    topic="lattice.result",
                    key=provider_id.encode('utf-8'),
                    value=json.dumps(response).encode('utf-8'),
                    callback=lambda err, msg, fed_id=federation_id: self._delivery_callback(err, msg, f"lattice_{fed_id}")
                )
                
                # Ensure the message is sent immediately
                self.producer.flush()
                transmission_time = self.monitor.stop_timer(f"lattice_transmission_{federation_id}")
                
                # Update metrics with transmission time
                metrics['transmission_time'] = transmission_time
                self.federation_metrics[federation_id] = metrics
                
                # Save metrics to Redis
                self._save_metrics_to_redis(federation_id, metrics)
                
                self.logger.info(f"=== COMPLETED PROVIDER CONFIGURATION ===")
                self.logger.info(f"Successfully processed configuration for federation {federation_id}")
                self.logger.info(f"Lattice size: {lattice_size} bytes")
                self.logger.info(f"Total processing time: {metrics['total_time']:.2f} seconds")
                
                return True
                
            except Exception as e:
                error_msg = f"Failed to send lattice result: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                raise RuntimeError(error_msg) from e
                
        except Exception as e:
            self.logger.error(f"Error in provider configuration: {str(e)}", exc_info=True)
            # Update federation status to failed
            if federation_id and federation_id in self.active_federations:
                self.active_federations[federation_id]['status'] = 'failed'
                self.active_federations[federation_id]['error'] = str(e)
            return False
    
    def _handle_federation_start(self, message):
        """
        Handle federation start notification
        """
        federation_id = message.get('federation_id')
        self.logger.info(f"Federation {federation_id} started")
        return True
    
    def _handle_federation_complete(self, message):
        """
        Handle federation completion notification
        """
        federation_id = message.get('federation_id')
        if federation_id in self.active_federations:
            self.active_federations[federation_id]['status'] = 'completed'
            self.logger.info(f"Federation {federation_id} completed")
        return True
        
    def _handle_lattice_request(self, message):
        """
        Handle lattice computation request
        """
        try:
            request_id = message.get('request_id')
            federation_id = message.get('federation_id')
            
            if not all([request_id, federation_id]):
                self.logger.error("Missing required fields in lattice request")
                return False
                
            self.logger.info(f"Processing lattice request {request_id} for federation {federation_id}")
            
            # Add request to processing queue
            if federation_id not in self.active_federations:
                self.active_federations[federation_id] = {'status': 'processing'}
                
            # TODO: Implement actual lattice computation
            # For now, just log the request
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing lattice request: {e}", exc_info=True)
            return False
            
    def _handle_model_update(self, message):
        """
        Handle model update from federation
        """
        try:
            federation_id = message.get('federation_id')
            model_data = message.get('model_data', {})
            
            if not federation_id:
                self.logger.error("Missing federation_id in model update")
                return False
                
            self.logger.info(f"Processing model update for federation {federation_id}")
            
            # Store the updated model
            if federation_id not in self.active_federations:
                self.active_federations[federation_id] = {}
                
            self.active_federations[federation_id]['last_model_update'] = time.time()
            self.active_federations[federation_id]['model_data'] = model_data
            
            # Log metrics about the model update
            metrics = {
                'federation_id': federation_id,
                'update_time': time.time(),
                'model_size': len(str(model_data)),
                'provider_id': self.actor_id
            }
            self._save_metrics_to_redis(federation_id, metrics)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing model update: {e}", exc_info=True)
            return False
            
    def _handle_federation_status(self, message):
        """
        Handle federation status update
        """
        try:
            federation_id = message.get('federation_id')
            status = message.get('status')
            
            if not all([federation_id, status]):
                self.logger.error("Missing required fields in federation status")
                return False
                
            self.logger.info(f"Updating status for federation {federation_id} to {status}")
            
            # Update federation status
            if federation_id not in self.active_federations:
                self.active_federations[federation_id] = {}
                
            self.active_federations[federation_id]['status'] = status
            
            # If federation is marked as complete, clean up resources
            if status.lower() in ['complete', 'completed', 'failed']:
                self._cleanup_federation(federation_id)
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing federation status: {e}", exc_info=True)
            return False
            
    def _cleanup_federation(self, federation_id):
        """
        Clean up resources for a completed or failed federation
        """
        try:
            if federation_id in self.active_federations:
                # Save final metrics before cleanup
                metrics = self.active_federations[federation_id].get('metrics', {})
                metrics['completion_time'] = time.time()
                metrics['status'] = 'completed'
                self._save_metrics_to_redis(federation_id, metrics)
                
                # Clean up federation data
                del self.active_federations[federation_id]
                self.logger.info(f"Cleaned up resources for federation {federation_id}")
                
        except Exception as e:
            self.logger.error(f"Error cleaning up federation {federation_id}: {e}", exc_info=True)
    
    def _load_dataset(self, dataset_path):
        """
        Load dataset from the specified path
        """
        try:
            if not os.path.exists(dataset_path):
                self.logger.error(f"Dataset file not found: {dataset_path}")
                return None
            
            with open(dataset_path, 'r') as file:
                data = file.read()
            
            # Parse based on file format (assuming FIMI format)
            return self._parse_fimi_data(data)
        except Exception as e:
            self.logger.error(f"Error loading dataset: {e}")
            return None
    
    def _parse_fimi_data(self, data):
        """
        Parse data in FIMI format
        """
        try:
            lines = data.strip().split('\n')
            transactions = []
            
            for line in lines:
                if line.strip() and not line.startswith('#'):
                    items = line.strip().split()
                    transactions.append(items)
            
            return transactions
        except Exception as e:
            self.logger.error(f"Error parsing FIMI data: {e}")
            return []
    
    def _compute_local_lattice(self, dataset, threshold):
        """
        Compute local concept lattice from dataset
        """
        try:
            if not dataset:
                self.logger.error("Cannot compute lattice: dataset is empty")
                return []
            
            # Create provider instance from core
            provider = core.Provider(1, dataset, threshold)
            
            # Compute local lattice
            return provider.compute_local_lattice()
        except Exception as e:
            self.logger.error(f"Error computing local lattice: {e}")
            return []
    
    def _delivery_callback(self, err, msg, context):
        """
        Callback for Kafka delivery reports
        """
        if err is not None:
            self.logger.error(f'Message delivery failed for {context}: {err}')
        else:
            self.logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')
            self.logger.info(f'Successfully delivered message for {context}')
    
    def _save_metrics_to_redis(self, federation_id, metrics):
        """
        Save metrics to Redis
        """
        try:
            if self.redis_client:
                metrics_key = f"metrics:{federation_id}:{self.actor_id}"
                self.redis_client.hmset(metrics_key, metrics)
                self.redis_client.expire(metrics_key, 86400)  # 24h TTL
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error saving metrics to Redis: {e}")
            return False
    
    def _delivery_callback(self, err, msg):
        """
        Callback for Kafka delivery confirmations
        """
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(f"Message delivered to {msg.topic()}")
    
    def _process_kafka_message(self, msg):
        """Process a single Kafka message with improved error handling"""
        try:
            self.logger.info(
                f"Received message from topic {msg.topic()}, "
                f"partition {msg.partition()}, offset {msg.offset()}"
            )
            
            # Process message asynchronously
            future = self.executor.submit(
                self._process_incoming_message, 
                msg.value().decode('utf-8')
            )
            
            def process_complete(f):
                try:
                    # Get the result to raise any exceptions
                    f.result()
                    # Commit the offset after successful processing
                    self.consumer.commit(message=msg, asynchronous=False)
                    self.logger.debug(f"Committed offset {msg.offset()} for {msg.topic()}[{msg.partition()}]")
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}", exc_info=True)
            
            future.add_done_callback(process_complete)
            
        except Exception as e:
            self.logger.error(f"Failed to process message: {e}", exc_info=True)
            raise
            
    def _reconnect_kafka(self):
        """Reconnect to Kafka broker"""
        self.logger.warning("Attempting to reconnect to Kafka...")
        try:
            if hasattr(self, 'consumer') and self.consumer is not None:
                self.consumer.close()
            
            # Reinitialize consumer with current config
            self.consumer = Consumer({
                'bootstrap.servers': self.kafka_servers,
                'group.id': f'alm_group_{self.actor_id}',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            })
            
            # Resubscribe to topics
            self.consumer.subscribe(self.subscribed_topics)
            self.consumer_healthy = True
            self.logger.info("Successfully reconnected to Kafka")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to reconnect to Kafka: {e}")
            self.consumer_healthy = False
            return False
            
    def _check_consumer_health(self):
        """Check if Kafka consumer is healthy"""
        if not hasattr(self, 'consumer') or self.consumer is None:
            return self._reconnect_kafka()
            
        try:
            # Simple poll to check connectivity
            self.consumer.poll(0.1)
            return True
            
        except Exception as e:
            self.logger.warning(f"Kafka connection error: {str(e)}")
            return self._reconnect_kafka()
            
    def start_listening(self):
        """Start the main Kafka message listening loop"""
        self.logger.info("Starting message listener")
        self.running = True
        poll_timeout = 1.0  # seconds
        health_check_interval = 30  # seconds
        last_health_check = time.time()
        
        try:
            while self.running:
                try:
                    # Check consumer health periodically
                    current_time = time.time()
                    if current_time - last_health_check > health_check_interval:
                        if not self._check_consumer_health():
                            time.sleep(5)
                            continue
                        last_health_check = current_time
                    
                    # Poll for messages
                    msg = self.consumer.poll(poll_timeout)
                    if msg is None:
                        continue
                        
                    # Process the message
                    self._process_kafka_message(msg)
                    
                except Exception as e:
                    self.logger.error(f"Error processing message: {str(e)}")
                    time.sleep(1)  # Prevent tight loop on errors
                    
        except Exception as e:
            self.logger.error(f"Fatal error in message loop: {str(e)}")
            raise
            
        finally:
            self.logger.info("Shutting down message listener")
            self.consumer_healthy = False
            if hasattr(self, 'consumer') and self.consumer is not None:
                self.consumer.close()
    
    def _verify_topics_exist(self, topics_to_verify=None):
        """Verify that all required topics exist and are accessible"""
        try:
            all_topics = self.consumer.list_topics(timeout=10.0)
            topics = topics_to_verify or self.subscribed_topics
            
            missing_topics = []
            available_topics = set(all_topics.topics.keys())
            
            for topic in topics:
                if topic not in available_topics:
                    missing_topics.append(topic)
                    self.logger.warning(f"Topic '{topic}' not found in available topics")
                else:
                    partitions = all_topics.topics[topic].partitions
                    self.logger.info(f"Topic '{topic}' has {len(partitions)} partitions")
            
            if missing_topics:
                self.logger.warning(f"Missing topics: {missing_topics}")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error verifying topics: {e}", exc_info=True)
            return False
    
    def _refresh_subscription(self):
        """Refresh the topic subscription"""
        try:
            self.logger.info(f"Refreshing subscription to topics: {self.subscribed_topics}")
            self.consumer.subscribe(self.subscribed_topics)
            
            # Force a poll to trigger the subscription
            msg = self.consumer.poll(1.0)
            if msg is None:
                self.logger.debug("No messages available after refresh")
            return True
            
        except Exception as e:
            self.logger.error(f"Error refreshing subscription: {e}", exc_info=True)
            return False
    
    def _handle_kafka_error(self, error):
        """Handle Kafka consumer errors"""
        self.logger.error(f"Kafka error: {error}")
        if error.code() == KafkaError._ALL_BROKERS_DOWN:
            self.logger.error("All brokers are down, attempting to reconnect...")
            self._reconnect_kafka()
        elif error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            self.logger.warning("Topic or partition error, will refresh subscription...")
            self._refresh_subscription()

def _verify_topics_exist(self, topics_to_verify=None):
    """Verify that all required topics exist and are accessible"""
    try:
        all_topics = self.consumer.list_topics(timeout=10.0)
        topics = topics_to_verify or self.subscribed_topics
        
        missing_topics = []
        available_topics = set(all_topics.topics.keys())
        
        for topic in topics:
            if topic not in available_topics:
                missing_topics.append(topic)
                self.logger.warning(f"Topic '{topic}' not found in available topics")
            else:
                partitions = all_topics.topics[topic].partitions
                self.logger.info(f"Topic '{topic}' has {len(partitions)} partitions")
        
        if missing_topics:
            self.logger.warning(f"Missing topics: {missing_topics}")
            return False
            
        return True
            
    except Exception as e:
        self.logger.error(f"Error verifying topics: {e}", exc_info=True)
        return False

def _refresh_subscription(self):
    """Refresh the topic subscription"""
    try:
        self.logger.info(f"Refreshing subscription to topics: {self.subscribed_topics}")
        self.consumer.subscribe(self.subscribed_topics)
        
        # Force a poll to trigger the subscription
        msg = self.consumer.poll(1.0)
        if msg is None:
            self.logger.debug("No messages available after refresh")
        return True
        
    except Exception as e:
        self.logger.error(f"Error refreshing subscription: {e}", exc_info=True)
        return False

    def _process_kafka_message(self, msg):
        """Process a single Kafka message and commit offset after successful processing
        
        Args:
            msg: The Kafka message to process
        """
        self.logger.info(f"Processing Kafka message{msg}")
        if not msg or not hasattr(msg, 'value'):
            self.logger.debug("Received empty or invalid message")
            return
    
        # Initialize variables for message metadata
        topic = partition = offset = None
        
        try:
            # Safely get message metadata
            try:
                topic = msg.topic() if hasattr(msg, 'topic') else 'unknown-topic'
                partition = msg.partition() if hasattr(msg, 'partition') else -1
                offset = msg.offset() if hasattr(msg, 'offset') else -1
                
                # Skip if this is a control message (e.g., subscription confirmation)
                if offset is None or offset < 0:
                    self.logger.debug(f"Received control message for topic {topic}")
                    return
                    
            except Exception as meta_err:
                self.logger.error(f"Error getting message metadata: {meta_err}", exc_info=True)
                return
            
            # Log message receipt with metadata
            self.logger.info(
                f"Processing message - Topic: {topic}, "
                f"Partition: {partition}, Offset: {offset}"
            )
            
            # Log message headers safely if they exist
            try:
                if hasattr(msg, 'headers') and msg.headers():
                    headers_dict = {k: v.decode('utf-8', 'replace') if isinstance(v, bytes) else v 
                                  for k, v in dict(msg.headers()).items()}
                    self.logger.debug(f"Message headers: {headers_dict}")
            except Exception as header_err:
                self.logger.warning(f"Failed to log message headers: {header_err}")
            
            # Log message key if it exists
            try:
                if hasattr(msg, 'key') and msg.key() is not None:
                    key = msg.key()
                    if isinstance(key, bytes):
                        key = key.decode('utf-8', errors='replace')
                    self.logger.debug(f"Message key: {key}")
            except Exception as key_err:
                self.logger.warning(f"Failed to log message key: {key_err}")
            
            # Process message content
            try:
                message_value = msg.value()
                if message_value is None:
                    self.logger.warning(f"Received message with None value from {topic}")
                    return
                
                # Parse message content
                try:
                    if isinstance(message_value, bytes):
                        try:
                            message_str = message_value.decode('utf-8')
                            try:
                                # Try to parse as JSON first
                                message = json.loads(message_str)
                                self.logger.debug("Successfully parsed message as JSON")
                            except json.JSONDecodeError:
                                # If not JSON, use as plain string
                                message = message_str
                                self.logger.debug("Message is not in JSON format, processing as plain text")
                        except UnicodeDecodeError as ude:
                            self.logger.error(f"Failed to decode message as UTF-8: {ude}")
                            return
                    else:
                        # Already a string or dict
                        if isinstance(message_value, str):
                            try:
                                message = json.loads(message_value)
                                self.logger.debug("Successfully parsed string message as JSON")
                            except json.JSONDecodeError:
                                message = message_value
                                self.logger.debug("Processing message as plain text")
                        else:
                            # Already a dict or other type
                            message = message_value
                    
                    # Log the processed message (truncated for large messages)
                    msg_str = str(message)
                    self.logger.debug(f"Processed message: {msg_str[:500]}{'...' if len(msg_str) > 500 else ''}")
                    
                    # Process the message with the decoded content
                    self._process_incoming_message(message)
                    
                    # Commit offsets periodically (every 5 seconds)
                    current_time = time.time()
                    if current_time - self.last_commit_time > 5.0:
                        try:
                            self.consumer.commit(asynchronous=False)
                            self.last_commit_time = current_time
                            self.logger.debug("Committed offsets to Kafka")
                        except Exception as commit_err:
                            self.logger.error(f"Error committing offsets: {commit_err}", exc_info=True)
                            # Continue processing even if commit fails
                    
                except Exception as process_err:
                    self.logger.error(f"Error processing message: {process_err}", exc_info=True)
                    # Don't store the offset if processing failed
                    return
                
            except Exception as value_err:
                self.logger.error(f"Error getting message value: {value_err}", exc_info=True)
                return
            
        except Exception as e:
            self.logger.error(
                f"Critical error in _process_kafka_message: {e}\n"
                f"Topic: {getattr(msg, 'topic', 'unknown')}, "
                f"Partition: {getattr(msg, 'partition', 'unknown')}, "
                f"Offset: {getattr(msg, 'offset', 'unknown')}",
                exc_info=True
            )
            # Don't re-raise to prevent the entire listener loop from crashing

def _shutdown_consumer(self):
    """Gracefully shut down the Kafka consumer"""
    self.logger.info("Shutting down message listener...")
    try:
        if hasattr(self, 'consumer') and self.consumer is not None:
            self.consumer.close()
            self.logger.info("Kafka consumer closed successfully")
    except Exception as e:
        self.logger.error(f"Error closing Kafka consumer: {e}")
    finally:
        self.logger.info("Message listener shut down")

def _heartbeat_loop(self):
    """
    Maintain Redis registration with periodic updates
    """
    while self.running:
        try:
            self.register_with_redis()
            time.sleep(60)  # Update registration every minute
        except Exception as e:
            self.logger.error(f"Error in heartbeat loop: {e}")
            time.sleep(5)  # Retry after 5 seconds on error

def _reconnect_kafka(self):
    """Attempt to reconnect to Kafka brokers with improved error handling"""
    max_retries = 5
    base_delay = 2  # Initial delay in seconds
    max_delay = 30  # Maximum delay between retries
    
    for attempt in range(max_retries):
        try:
            self.logger.info(f"Attempting to reconnect to Kafka (attempt {attempt + 1}/{max_retries})...")
            
            # Close existing consumer if it exists
            if hasattr(self, 'consumer') and self.consumer is not None:
                try:
                    self.logger.debug("Closing existing Kafka consumer...")
                    self.consumer.close()
                    self.logger.debug("Successfully closed existing Kafka consumer")
                except Exception as close_error:
                    self.logger.warning(f"Error closing existing consumer: {close_error}")
                finally:
                    self.consumer = None
            
            # Reinitialize consumer with fresh configuration
            self.logger.info("Initializing new Kafka consumer...")
            
            # Recreate the consumer configuration
            consumer_config = {
                'bootstrap.servers': self.kafka_servers,
                'group.id': f'alm_group_{self.actor_id}',
                'client.id': f'alm_actor_{self.actor_id}_{uuid.uuid4().hex[:8]}',  # Add unique suffix
                # 'auto.offset.reset': 'earliest',
                # 'enable.auto.commit': False,
                # 'enable.auto.offset.store': False,
                # 'session.timeout.ms': 30000,
                # 'heartbeat.interval.ms': 10000,
                # 'max.poll.interval.ms': 300000,
                # 'fetch.min.bytes': 1,
                # 'fetch.wait.max.ms': 500,
                # 'socket.timeout.ms': 30000,
                # 'api.version.request': True,
                # 'api.version.fallback.ms': 0,
                # 'broker.version.fallback': '2.0.0',
                # 'reconnect.backoff.ms': 1000,
                # 'reconnect.backoff.max.ms': 60000
                }
                
                # Create new consumer
                self.consumer = Consumer(consumer_config)
                
                # Verify we can list topics before subscribing
                try:
                    topics = self.consumer.list_topics(timeout=10.0)
                    self.logger.info(f"Successfully connected to Kafka broker. Available topics: {list(topics.topics.keys())}")
                    
                    # Check if our topics exist
                    for topic in self.subscribed_topics:
                        if topic in topics.topics:
                            self.logger.info(f"Topic '{topic}' has {len(topics.topics[topic].partitions)} partitions")
                        else:
                            self.logger.warning(f"Topic '{topic}' not found in available topics")
                    
                except Exception as e:
                    self.logger.error(f"Failed to list topics: {e}")
                    raise
                
                # Resubscribe to topics
                self.logger.info(f"Subscribing to topics: {self.subscribed_topics}")
                try:
                    self.consumer.subscribe(self.subscribed_topics)
                    # Force a poll to trigger the subscription
                    msg = self.consumer.poll(1.0)
                    if msg is None:
                        self.logger.info("Subscription successful, no messages available yet")
                    self.logger.info("Successfully re-subscribed to topics")
                    self.consumer_healthy = True
                    return True
                except Exception as e:
                    self.logger.error(f"Failed to subscribe to topics: {e}")
                    raise
                self.consumer.subscribe(self.subscribed_topics, on_assign=self.on_assign)
                
                # Force a poll to trigger the subscription
                msg = self.consumer.poll(1.0)
                if msg is None:
                    self.logger.debug("No messages available after reconnection")
                
                # Verify subscription
                subscription = self.consumer.subscription()
                self.logger.info(f"Successfully reconnected to Kafka. Current subscription: {subscription}")
                
                # Verify topics exist and are accessible
                if not self._verify_topics_exist():
                    self.logger.warning("Some subscribed topics are not available after reconnection")
                
                self.consumer_healthy = True
                return True
                
            except Exception as e:
                self.logger.error(f"Reconnection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                
        self.logger.error("Failed to reconnect to Kafka after multiple attempts")
        return False
    
    def stop_actor(self):
        """
        Gracefully stop the ALM actor
        """
        self.logger.info("Stopping ALM actor")
        self.running = False
        
        # Clean up resources
        try:
            # Cancel any remaining tasks
            self.executor.shutdown(wait=False)
            
            # Close Kafka consumer
            try:
                self.consumer.close()
            except Exception as e:
                self.logger.error(f"Error closing consumer: {e}")
            
            # Remove from Redis
            if self.redis_client:
                try:
                    self.redis_client.hdel("federated_providers", self.actor_id)
                    self.logger.info(f"Removed provider {self.actor_id} from Redis")
                except Exception as e:
                    self.logger.error(f"Error removing from Redis: {e}")
            
            self.logger.info("ALM actor stopped")
        except Exception as e:
            self.logger.error(f"Error stopping actor: {e}")

if __name__ == '__main__':
    # Use environment variable or default to the running Kafka broker
    kafka_servers = os.getenv("KAFKA_BROKERS", "kafka-1:9092")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('alm_actor.log')
        ]
    )
    
    # Configure Kafka logger to only show errors
    logging.getLogger('kafka').setLevel(logging.ERROR)
    logger = logging.getLogger("ALM_MAIN")
    logger.info(f"Starting ALM with Kafka servers: {kafka_servers}")
    
    # Initialize and start the ALM actor
    try:
        alm = ALMActor(kafka_servers)
        logger.info("ALM actor initialized successfully")
        alm.start_listening()
    except Exception as e:
        logger.error(f"Failed to start ALM actor: {e}", exc_info=True)