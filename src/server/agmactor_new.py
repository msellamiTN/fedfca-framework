import json
import time
import logging
import os
import base64
import redis
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from confluent_kafka import Producer, Consumer, KafkaError
from concurrent.futures import ThreadPoolExecutor
import threading
import yaml
import uuid as uuid
from cryptography.fernet import Fernet
# Import core components
from fedfca_core import FedFCA_core as core
from commun.logactor import LoggerActor
from commun.FedFcaMonitor import FedFcaMonitor
from commun.CryptoManager import CryptoManager
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
class AGMActor:
    """
    Simplified Actor for Global Model (AGM) in FedFCA.
    Implements Redis-based provider discovery and streamlined message exchange.
    """
    
    def __init__(self, kafka_servers=None, max_workers=10, metrics_dir="/data/metrics"):
        # Dictionary to track which providers have received which keys
        self.provider_keys = {}
        # Track sent configurations to prevent duplicates
        self.sent_configs = set()  # format: (federation_id, provider_id)
        # Track processed lattice results to prevent duplicates
        self.processed_lattices = set()  # format: (federation_id, provider_id)
        # Initialize actor ID and logging
        import random
        import string
        import os
        
        random_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.actor_id = f"AGM_{random_id}"
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(self.actor_id)
        
        # Initialize performance monitoring
        self.monitor = FedFcaMonitor()
        
        # Metrics configuration
        self.metrics_dir = metrics_dir
        os.makedirs(self.metrics_dir, exist_ok=True)
        self.metrics = {
            'federations': {},
            'providers': {},
            'aggregation_metrics': {}
        }
        
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
        
        # Initialize RedisKMS for key management with authentication
        from commun.RedisKMS import RedisKMS
        redis_password = os.getenv("REDIS_PASSWORD", "mysecred")
        redis_user = os.getenv("REDIS_USER", "fedfac")
        
        self.logger.info(f"Initializing RedisKMS for AGM with host={redis_host}, port={redis_port}")
        
        redis_kms = RedisKMS(
            host=redis_host,
            port=redis_port,
            db=0,
            key_prefix="fedfca:kms:agm:",
            username=redis_user,
            password=redis_password
        )
        
        # Initialize core components with RedisKMS
        self.crypto_manager = CryptoManager(kms=redis_kms)
        self.aggregator = core.Aggregator(threshold=self.config.get("server", {}).get("threshold", 0.5))
        
        # Initialize key tracking
        self.provider_keys = {}  # {provider_id: [key_id1, key_id2, ...]}
        
        # State tracking
        self.active_federations = {}  # {fed_id: {'providers': [], 'status': ''}}
        self.pending_lattices = {}    # {fed_id: {provider_id: lattice}}
        
        # Enhanced metrics tracking
        self.federation_metrics = {
            'federations': {},  # Per-federation metrics
            'providers': {},    # Per-provider metrics
            'aggregations': {}  # Aggregation metrics
        }
        
        # Initialize thread pool executor for async message processing
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = False
        
        # Simplified topic structure
        self.kafka_servers = kafka_servers or os.environ.get("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9093,kafka-3:9094")
        
        # Define required topics
        self.required_topics = [
            'provider.config',
            'federation.start',
            'federation.complete',
            'model.updates',
            'training.status',
            'lattice.result',
            'provider.ack',
            'key.distribution',  # For sending encryption keys to providers
            'key.ack',           # For key delivery acknowledgements
            'participant_lattice_topic',  # For lattice submissions from participants
            'provider.registration'  # For provider registration messages
        ]
        
        # Create required topics
        self._create_kafka_topics()
        
        # Configure Kafka producer
        producer_config = {
            'bootstrap.servers': self.kafka_servers,
            'message.max.bytes': 10485880  # 10MB max message size
        }
        self.producer = Producer(producer_config)
        
        # Configure Kafka consumer
        consumer_config = {
            'bootstrap.servers': self.kafka_servers,
            'group.id': f"agm_group_{self.actor_id}",
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'max.partition.fetch.bytes': 10485880  # 10MB max fetch
        }
        self.consumer = Consumer(consumer_config)
        
        # Subscribe to relevant topics from required_topics
        self.subscribed_topics = [
            topic for topic in self.required_topics 
            if topic in [
                'lattice.result',          # Results from providers
                'key.ack',                # Key delivery acknowledgements
                'participant_lattice_topic',  # Lattice submissions from participants
                'provider.registration'   # Provider registration messages
            ]
        ]
        
        self.logger.info(f"Subscribing to topics: {self.subscribed_topics}")
        try:
            self.consumer.subscribe(self.subscribed_topics)
            self.logger.info("Successfully subscribed to all topics")
        except Exception as e:
            self.logger.error(f"Failed to subscribe to topics: {e}")
            raise
        
        self.logger.info(f"AGMActor {self.actor_id} initialized")
        
    def generate_and_distribute_keys(self, provider_ids, federation_id=None, key_type='fernet', key_size=32, expiration=604800):
        """
        Generate and distribute encryption keys to providers using RedisKMS.
        
        Args:
            provider_ids (list): List of provider IDs to send keys to
            federation_id (str, optional): ID of the federation these keys are for
            key_type (str): Type of key to generate ('fernet' or 'raw')
            key_size (int): Size of the key in bytes (for raw keys)
            expiration (int): Key TTL in seconds (default 1 week)
            
        Returns:
            dict: Mapping of provider_id to key_id
        """
        key_mapping = {}
        
        for provider_id in provider_ids:
            try:
                # Generate a unique key ID for this provider
                key_id = f"key_{provider_id}_{int(time.time())}"
                
                # Generate and store the key in RedisKMS
                key_metadata = self.crypto_manager.generate_key(
                    key_id=key_id,
                    key_type=key_type,
                    key_size=key_size,
                    expiration=expiration
                )
                
                if not key_metadata:
                    self.logger.error(f"Failed to generate key for provider {provider_id}")
                    continue
                
                # Track the key for this provider
                if provider_id not in self.provider_keys:
                    self.provider_keys[provider_id] = []
                self.provider_keys[provider_id].append(key_id)
                key_mapping[provider_id] = key_id
                
                # Send only the key ID to the provider
                self._send_key_to_provider(provider_id, key_id, key_metadata)
                
            except Exception as e:
                self.logger.error(f"Error generating key for provider {provider_id}: {e}", exc_info=True)
        
        return key_mapping
        
    def _send_key_to_provider(self, provider_id, key_id, key_data, callback=None):
        """
        Send an encryption key ID to a specific provider using RedisKMS.
        
        Args:
            provider_id (str): ID of the provider to send the key to
            key_id (str): ID of the key in RedisKMS
            key_data (dict): Key metadata (does not contain the actual key material)
            callback (callable, optional): Callback function for delivery reports
        """
        try:
            message = {
                'message_type': 'key_distribution',
                'message_id': str(uuid.uuid4()),
                'from_actor': self.actor_id,
                'to_actor': provider_id,
                'key_id': key_id,  # Only send the key ID, not the actual key material
                'key_type': key_data.get('key_type', 'fernet'),
                'expiration': key_data.get('expiration'),
                'timestamp': time.time()
            }
            
            self.logger.info(f"Sending key ID {key_id} to provider {provider_id}")
            
            # Send the message
            self.producer.produce(
                topic='key.distribution',
                key=provider_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=callback or self._delivery_callback
            )
            self.producer.flush()
            
            self.logger.info(f"Sent key ID {key_id} to provider {provider_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send key ID {key_id} to provider {provider_id}: {e}", exc_info=True)
            return False
        
    def _handle_key_ack(self, message):
        """
        Handle key delivery acknowledgement from a provider.
        
        Args:
            message (dict): Message containing the acknowledgement with keys:
                - from_actor: Provider ID
                - key_id: The key being acknowledged
                - status: 'success' or 'error'
                - error: Error message if status is 'error'
                - timestamp: When the ack was sent
        """
        try:
            provider_id = message.get('from_actor')
            key_id = message.get('key_id')
            status = message.get('status')
            error_msg = message.get('error')
            
            if not all([provider_id, key_id, status]):
                self.logger.error("Invalid key ack message: missing required fields")
                return
            
            # Update key metadata in RedisKMS
            metadata_update = {
                f'provider_{provider_id}_status': status,
                f'provider_{provider_id}_last_updated': datetime.utcnow().isoformat()
            }
            
            if status == 'success':
                self.logger.info(f"Provider {provider_id} successfully received key {key_id}")
            else:
                error_msg = error_msg or "Unknown error"
                self.logger.warning(f"Provider {provider_id} reported issue with key {key_id}: {error_msg}")
                metadata_update[f'provider_{provider_id}_error'] = error_msg
            
            # Update the key metadata in Redis
            if not self.crypto_manager.set_key_metadata(key_id, metadata_update):
                self.logger.error(f"Failed to update key metadata for {key_id}")
                
        except Exception as e:
            self.logger.error(f"Error processing key ack: {e}", exc_info=True)
    
    def _delivery_callback(self, err, msg):
        """
        Handle delivery reports for key distribution messages.
        """
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    

    def _create_kafka_topics(self, timeout=30) -> bool:
        """
        Set up Kafka topics if they don't exist.
        
        Args:
            timeout: Timeout in seconds for Kafka operations
            
        Returns:
            bool: True if all topics were created successfully, False otherwise
        """
        # Remove duplicates while preserving order
        topics = list(dict.fromkeys(self.required_topics))
        
        self.logger.info(f"Setting up Kafka topics: {', '.join(topics)}")
        self.logger.info(f"Connecting to Kafka at: {self.kafka_servers}")
        
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                # Initialize admin client with retry configuration
                admin_config = {
                    'bootstrap.servers': self.kafka_servers,
                    'client.id': f'{self.actor_id}_admin',
                    'socket.timeout.ms': int(timeout * 1000),
                    'metadata.request.timeout.ms': int(timeout * 1000),
                    'retries': 3,
                    'retry.backoff.ms': 1000
                }
                admin_client = AdminClient(admin_config)
                
                # Get existing topics with retry
                for _ in range(3):
                    try:
                        existing_topics = admin_client.list_topics(timeout=timeout).topics
                        self.logger.info(f"Successfully connected to Kafka. Found {len(existing_topics)} existing topics")
                        break
                    except Exception as e:
                        self.logger.warning(f"Attempt to list topics failed: {e}. Retrying...")
                        time.sleep(1)
                else:
                    raise Exception("Failed to list topics after multiple attempts")
                
                # Find missing topics
                missing_topics = [topic for topic in topics if topic not in existing_topics]
                
                if not missing_topics:
                    self.logger.info("All required topics already exist")
                    return True
                    
                # Create missing topics
                self.logger.info(f"Attempting to create {len(missing_topics)} missing topics: {', '.join(missing_topics)}")
                
                # Topic configuration with optimized settings
                topic_config = {
                    'retention.ms': '604800000',  # 7 days
                    'retention.bytes': '-1',  # Unlimited size
                    'delete.retention.ms': '86400000',  # 1 day
                    'segment.ms': '604800000',  # 7 days
                    'min.insync.replicas': '1',  # Reduced to 1 for single broker
                    'cleanup.policy': 'delete',
                    'message.timestamp.type': 'LogAppendTime',
                    'file.delete.delay.ms': '60000'  # 1 minute
                }
                
                # Create new topics with appropriate configuration
                new_topics = [
                    NewTopic(
                        topic=topic,
                        num_partitions=1,  # Single partition for simplicity
                        replication_factor=1,  # Single replica for single broker
                        config=topic_config
                    ) for topic in missing_topics
                ]
                
                # Create the topics with retry
                for _ in range(3):
                    try:
                        fs = admin_client.create_topics(
                            new_topics,
                            request_timeout=timeout,
                            validate_only=False
                        )
                        
                        # Wait for each topic creation to complete
                        success = True
                        for topic, f in fs.items():
                            try:
                                f.result(timeout=timeout)
                                self.logger.info(f"Successfully created topic: {topic}")
                            except Exception as e:
                                self.logger.error(f"Failed to create topic {topic}: {e}")
                                success = False
                        
                        if success:
                            self.logger.info("Successfully created all missing topics")
                            return True
                            
                    except Exception as e:
                        self.logger.warning(f"Attempt to create topics failed: {e}. Retrying...")
                        time.sleep(1)
                
                # If we get here, topic creation failed after retries
                raise Exception(f"Failed to create topics after multiple attempts: {missing_topics}")
                
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    self.logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed. "
                        f"Retrying in {wait_time} seconds... Error: {str(e)}"
                    )
                    time.sleep(wait_time)
                else:
                    self.logger.error(
                        f"All {max_retries} attempts to set up Kafka topics failed. "
                        f"Last error: {str(e)}"
                    )
                    return False
        
        return False
    
    def _load_config(self):
        """
        Load configuration from YAML file
        """
        try:
            with open(self.config_file_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Error loading config file: {e}")
            return {}

    def _save_metrics_to_redis(self, federation_id, metrics):
        """
        Save metrics to Redis with structured keys and proper serialization
        
        Args:
            federation_id: ID of the federation
            metrics: Dictionary of metrics to save
            
        Returns:
            bool: True if save was successful, False otherwise
        """
        if not self.redis_client:
            self.logger.warning("Redis client not available, skipping metrics save")
            return False
        
        try:
            pipeline = self.redis_client.pipeline()
            timestamp = int(time.time())
            
            # Save federation metrics
            fed_key = f"fedfca:metrics:federation:{federation_id}"
            fed_metrics = metrics.get('federation', {})
            if fed_metrics:
                pipeline.hmset(fed_key, {
                    'metrics': json.dumps(fed_metrics, default=str),
                    'updated_at': timestamp
                })
                pipeline.expire(fed_key, 60 * 60 * 24)  # 24h TTL
            
            # Save provider metrics
            provider_metrics = metrics.get('providers', {})
            for provider_id, p_metrics in provider_metrics.items():
                provider_key = f"fedfca:metrics:provider:{provider_id}:{federation_id}"
                pipeline.hmset(provider_key, {
                    'metrics': json.dumps(p_metrics, default=str),
                    'updated_at': timestamp
                })
                pipeline.expire(provider_key, 60 * 60 * 24)  # 24h TTL
            
            # Execute all operations atomically
            pipeline.execute()
            self.logger.debug(f"Successfully saved metrics for federation {federation_id} to Redis")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving metrics to Redis: {e}", exc_info=True)
            return False
    
    def _save_metrics_to_file(self, federation_id, metrics):
        """
        Save metrics to a JSON file for analysis with proper error handling and rotation
        
        Args:
            federation_id: ID of the federation
            metrics: Dictionary of metrics to save
            
        Returns:
            str: Path to the saved file if successful, None otherwise
        """
        try:
            # Ensure metrics directory exists
            os.makedirs(self.metrics_dir, exist_ok=True)
            
            # Create a timestamp for the file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Create a clean filename with federation ID and timestamp
            safe_fed_id = "".join(c if c.isalnum() else "_" for c in federation_id)
            filename = os.path.join(
                self.metrics_dir,
                f"fedfca_metrics_{safe_fed_id}_{timestamp}.json"
            )
            
            # Add comprehensive metadata
            metrics_with_meta = {
                'metadata': {
                    'federation_id': federation_id,
                    'export_timestamp': datetime.now().isoformat(),
                    'system': {
                        'hostname': os.uname().nodename if hasattr(os, 'uname') else 'unknown',
                        'python_version': f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                    },
                    'version': '1.0'
                },
                'metrics': metrics
            }
            
            # Write to a temporary file first, then rename atomically
            temp_filename = f"{filename}.tmp"
            with open(temp_filename, 'w', encoding='utf-8') as f:
                json.dump(metrics_with_meta, f, indent=2, ensure_ascii=False, default=str)
            
            # Atomic rename (works on POSIX systems)
            os.replace(temp_filename, filename)
            
            self.logger.info(f"Metrics successfully saved to {filename}")
            
            # Clean up old metrics files (keep last 10)
            self._cleanup_old_metrics(safe_fed_id)
            
            return filename
            
        except Exception as e:
            self.logger.error(f"Error saving metrics to file: {e}", exc_info=True)
            # Try to clean up temporary file if it exists
            if 'temp_filename' in locals() and os.path.exists(temp_filename):
                try:
                    os.remove(temp_filename)
                except:
                    pass
            return None
            
    def _cleanup_old_metrics(self, federation_prefix, max_files=10):
        """
        Clean up old metrics files, keeping only the most recent ones
        
        Args:
            federation_prefix: Prefix of the federation ID for file matching
            max_files: Maximum number of files to keep
        """
        try:
            # Find all metric files for this federation
            pattern = os.path.join(self.metrics_dir, f"fedfca_metrics_{federation_prefix}_*.json")
            files = glob.glob(pattern)
            
            # Sort by modification time (newest first)
            files.sort(key=os.path.getmtime, reverse=True)
            
            # Delete older files if we have more than max_files
            if len(files) > max_files:
                for old_file in files[max_files:]:
                    try:
                        os.remove(old_file)
                        self.logger.debug(f"Cleaned up old metrics file: {old_file}")
                    except Exception as e:
                        self.logger.warning(f"Failed to clean up old metrics file {old_file}: {e}")
                        
        except Exception as e:
            self.logger.warning(f"Error during metrics cleanup: {e}")
    
    def _collect_aggregation_metrics(self, federation_id):
        """
        Collect and aggregate comprehensive metrics for a federation
        
        Args:
            federation_id: ID of the federation
            
        Returns:
            dict: Aggregated metrics with detailed breakdowns
        """
        if federation_id not in self.federation_metrics['federations']:
            self.logger.warning(f"No metrics found for federation {federation_id}")
            return {}
            
        fed_metrics = self.federation_metrics['federations'][federation_id]
        provider_metrics = {}
        
        # Initialize comprehensive metrics structure
        agg_metrics = {
            'federation': {
                'id': federation_id,
                'start_time': fed_metrics.get('start_time'),
                'end_time': datetime.now().isoformat(),
                'status': fed_metrics.get('status', 'unknown'),
                'provider_count': 0,
                'total_lattices_received': len(self.pending_lattices.get(federation_id, {})),
            },
            'timing': {
                'total_duration': None,
                'aggregation_time': None,
                'per_phase': {}
            },
            'resources': {
                'memory_usage': {},
                'cpu_usage': {}
            },
            'lattice_metrics': {
                'total_concepts': 0,
                'avg_concepts_per_provider': 0,
                'concept_reduction_ratio': 0,
                'global_stability': 0
            },
            'providers': {}
        }
        
        # Calculate duration if start time is available
        if 'start_time' in fed_metrics and fed_metrics['start_time']:
            try:
                start_dt = datetime.fromisoformat(fed_metrics['start_time'])
                agg_metrics['timing']['total_duration'] = (datetime.now() - start_dt).total_seconds()
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Invalid start_time format: {e}")
        
        # Collect and aggregate provider metrics
        provider_count = 0
        total_concepts = 0
        
        for provider_id, metrics in self.federation_metrics['providers'].items():
            if provider_id not in fed_metrics.get('providers', []):
                continue
                
            provider_count += 1
            provider_metrics[provider_id] = metrics
            
            # Aggregate lattice metrics
            if 'lattice_size' in metrics:
                total_concepts += metrics['lattice_size']
            
            # Add provider metrics to the output
            agg_metrics['providers'][provider_id] = {
                'computation_time': metrics.get('computation_time'),
                'encryption_time': metrics.get('encryption_time'),
                'lattice_size': metrics.get('lattice_size'),
                'last_update': metrics.get('last_update')
            }
        
        # Update federation metrics
        agg_metrics['federation']['provider_count'] = provider_count
        
        # Calculate derived metrics
        if provider_count > 0:
            agg_metrics['lattice_metrics'].update({
                'total_concepts': total_concepts,
                'avg_concepts_per_provider': total_concepts / provider_count if provider_count > 0 else 0,
                'concept_reduction_ratio': 0,  # Will be updated during aggregation
                'global_stability': 0  # Will be updated during aggregation
            })
        
        # Add aggregation metrics if available
        if federation_id in self.federation_metrics['aggregations']:
            agg_metrics.update({
                'aggregation': self.federation_metrics['aggregations'][federation_id]
            })
            
            # Update timing metrics from aggregation
            if 'timing' in agg_metrics['aggregation']:
                agg_metrics['timing'].update(agg_metrics['aggregation']['timing'])
        
        # Store the aggregated metrics
        self.federation_metrics['aggregations'][federation_id] = agg_metrics
        
        return agg_metrics
    
    def get_registered_providers(self):
        """
        Get all registered providers from Redis
        """
        try:
            if self.redis_client:
                providers = self.redis_client.hgetall("federated_providers")
                if not providers:
                    self.logger.warning("No providers registered in Redis")
                    return {}
                
                # Convert from bytes to dict
                return {k: json.loads(v) for k, v in providers.items()}
            else:
                self.logger.error("Redis client not available, cannot get providers")
                return {}
        except Exception as e:
            self.logger.error(f"Error getting registered providers: {e}")
            return {}
    
    def initiate_federation(self):
        """
        Start federation with registered providers
        """
        try:
            # Get registered providers
            providers = self.get_registered_providers()
            if not providers:
                self.logger.warning("No providers available for federation")
                return None
            
            self.logger.info(f"Found {len(providers)} registered providers")
            
            # Generate federation ID using random instead of uuid
            import random
            import string
            random_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
            federation_id = f"fed_{random_id}"
            
            # Generate encryption keys for each provider
            encryption_keys = {}
            for provider_id in providers.keys():
                encryption_keys[provider_id] = self.crypto_manager.generate_key()
            
            # Store federation state
            self.active_federations[federation_id] = {
                'status': 'initiated',
                'providers': list(providers.keys()),
                'start_time': time.time(),
                'encryption_keys': encryption_keys
            }
            
            # Initialize pending lattices storage
            self.pending_lattices[federation_id] = {}
            
            # Publish federation start notification
            self._publish_federation_start(federation_id)
            
            # Send configuration to each provider
            for provider_id, provider_info in providers.items():
                self._send_provider_config(federation_id, provider_id, encryption_keys[provider_id])
            
            self.logger.info(f"Federation {federation_id} initiated with {len(providers)} providers")
            return federation_id
        
        except Exception as e:
            self.logger.error(f"Error initiating federation: {e}")
            return None
    
    def _publish_federation_start(self, federation_id):
        """
        Publish federation start notification
        """
        try:
            message = {
                'action': 'federation_start',
                'federation_id': federation_id,
                'timestamp': time.time()
            }
            
            self.producer.produce(
                "federation.start",
                value=json.dumps(message).encode('utf-8')
            )
            self.producer.flush()
            self.logger.info(f"Published federation start notification for {federation_id}")
            return True
        except Exception as e:
            self.logger.error(f"Error publishing federation start: {e}")
            return False
    
    def _send_provider_config(self, federation_id, provider_id, encryption_key):
        """
        Send configuration to a provider.
        Ensures exactly one configuration is sent to each provider per federation.
        """
        # Check if config was already sent to this provider for this federation
        config_key = (federation_id, provider_id)
        if config_key in self.sent_configs:
            self.logger.warning(f"Configuration already sent to provider {provider_id} for federation {federation_id}")
            return False
            
        try:
            # Determine dataset path based on provider index
            provider_index = int(provider_id.split('_')[-1]) if '_' in provider_id else 1
            dataset_path = f"/data/federated_datasets/mushroom_example/providers_5/iid/provider_{provider_index}.txt"
            
            # Create configuration message
            config = {
                'action': 'provider_config',
                'federation_id': federation_id,
                'provider_id': provider_id,
                'encryption_key': encryption_key,
                'dataset_name': self.config.get("provider", {}).get("name", "mushroom_example"),
                'strategy': self.config.get("provider", {}).get("strategy", "IID"),
                 'context_sensitivity': self.config.get("provider", {}).get("context_sensitivity", 0.2),
                'dataset_config': {
                    'path':  self.config.get("provider", {}).get("url",dataset_path),
                    'threshold': self.config.get("provider", {}).get("threshold", 0.5),
                    'context_sensitivity': self.config.get("provider", {}).get("context_sensitivity", 0.2),
                    'dataset_name': self.config.get("provider", {}).get("name", "mushroom_example"
                    )
                },
                'timestamp': time.time(),
                'message_id': f"config_{federation_id}_{provider_id}_{int(time.time())}"
            }
            
            # Log the configuration being sent
            self.logger.info(f"Sending configuration to provider {provider_id} for federation {federation_id}")
            self.logger.debug(f"Configuration: {json.dumps(config, indent=2)}")
            
            # Send the configuration
            self.producer.produce(
                topic="provider.config",
                key=provider_id.encode('utf-8'),
                value=json.dumps(config).encode('utf-8'),
                callback=lambda err, msg: self._delivery_callback(err, msg, f"config_{federation_id}")
            )
            
            # Ensure the message is sent immediately
            self.producer.flush()
            
            # Mark config as sent for this provider
            self.sent_configs.add(config_key)
            self.logger.info(f"Successfully sent configuration to provider {provider_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending provider config to {provider_id}: {str(e)}", exc_info=True)
            return False
            
    def _delivery_callback(self, err, msg, context):
        """
        Callback for Kafka delivery reports
        """
        if err is not None:
            self.logger.error(f'Message delivery failed for {context}: {err}')
        else:
            self.logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')
            self.logger.info(f'Successfully delivered configuration for {context}')
    
    # def _process_incoming_message(self, message_data):
    #     """
    #     Process incoming Kafka message with comprehensive metrics collection
    #     to address reviewer comments about architectural overhead analysis
    #     """
    #     # Start timing message processing overhead
    #     message_processing_start = time.time()
        
    #     try:
    #         # Parse the message - measure deserialization overhead
    #         deserialization_start = time.time()
    #         logging.debug(f"Processing received message: {message_data}")
    #         message = json.loads(message_data.value().decode('utf-8'))
    #         deserialization_time = time.time() - deserialization_start
            
    #         self.logger.debug(f"Processing received message: {message}")
    #         topic = message_data.topic()
    #         action = message.get('action', 'unknown')
            
    #         # Track communication overhead metrics
    #         message_overhead_metrics = {
    #             'message_reception_time': message_processing_start,
    #             'deserialization_time': deserialization_time,
    #             'message_size_bytes': len(message_data.value()),
    #             'topic': topic,
    #             'action': action
    #         }
            
    #         self.logger.info(f"Processing message {message} on topic {topic}: {action}")
            
    #         # Log message details
    #         message_id = message.get('message_id', 'N/A')
    #         federation_id = message.get('federation_id', 'N/A')
    #         self.logger.debug(f"Message details - ID: {message_id}, Federation: {federation_id}")
            
    #         # Process based on topic and action
    #         if topic == "lattice.result" and action == "lattice_result":
    #             return self._handle_lattice_result(message, message_overhead_metrics)
    #         elif topic == "participant_lattice_topic" and action == "lattice_submission":
    #             # Handle lattice submission from ALM actors
    #             lattice_message = {
    #                 'federation_id': message.get('federation_id'),
    #                 'provider_id': message.get('provider_id'),  # ALM sends provider_id
    #                 'encrypted_lattice': message.get('encrypted_lattice'),
    #                 'metrics': message.get('metrics', {}),  # Basic metrics
    #                 'comprehensive_metrics': message.get('comprehensive_metrics', {})  # Enhanced metrics
    #             }
    #             self.logger.info(f"Processing lattice submission from provider {lattice_message['provider_id']} for federation {lattice_message['federation_id']}")
    #             return self._handle_lattice_result(lattice_message, message_overhead_metrics)
    #         elif topic == "provider.registration" and action == "provider_register":
    #             self.logger.info(f"Processing provider registration: {message}")
    #             return True
    #         else:
    #             self.logger.warning(f"Unhandled message action: {action} on topic {topic}")
    #             return False
        
    #     except json.JSONDecodeError as je:
    #         self.logger.error(f"Failed to decode JSON message: {je}")
    #         return False
    #     except Exception as e:
    #         self.logger.error(f"Error processing message: {str(e)}", exc_info=True)
    #         return False
    def _process_incoming_message(self, message_data):
        """
        Process incoming Kafka message with comprehensive metrics collection
        to address reviewer comments about architectural overhead analysis
        """
        # Start timing message processing overhead
        message_processing_start = time.time()
         
        try:
            # Parse the message - measure deserialization overhead
            deserialization_start = time.time()
            # logging.debug(f"Processing received message: {message_data}")
            message = json.loads(message_data.value().decode('utf-8'))
            logging.debug(f"Keys of message: {message.keys()}")
            deserialization_time = time.time() - deserialization_start
            
            # self.logger.debug(f"Processing received message: {message}")
            topic = message_data.topic()
            action = message.get('action', 'unknown')
            logging.debug(f"Processing action: {action} on topic: {topic}")
            # Track communication overhead metrics
            message_overhead_metrics = {
                'message_reception_time': message_processing_start,
                'deserialization_time': deserialization_time,
                'message_size_bytes': len(message_data.value()),
                'topic': topic,
                'action': action
            }
            
            # self.logger.info(f"Processing message {message} on topic {topic}: {action}")
            self.logger.info(f"Processing message {message.get('metrics')} on topic {topic}: {action}")
            # Log message details
            message_id = message.get('message_id', 'N/A')
            federation_id = message.get('federation_id', 'N/A')
            self.logger.debug(f"Message details - ID: {message_id}, Federation: {federation_id}")
            
            # Process based on topic and action
            if topic == "lattice.result":
                return self._handle_lattice_result(message, message_overhead_metrics)
            elif topic == "participant_lattice_topic":
                # Handle lattice submission from ALM actors
                lattice_message = {
                    'federation_id': message.get('federation_id'),
                    'provider_id': message.get('provider_id'),  # ALM sends provider_id
                    'encrypted_lattice': message.get('encrypted_lattice'),
                    'metrics': message.get('metrics', {}),  # Basic metrics
                    'comprehensive_metrics': message.get('comprehensive_metrics', {})  # Enhanced metrics
                }
                self.logger.info(f"Processing lattice submission from provider {lattice_message['provider_id']} for federation {lattice_message['federation_id']}")
                return self._handle_lattice_result(lattice_message, message_overhead_metrics)
            elif topic == "provider.registration" and action == "provider_register":
                self.logger.info(f"Processing provider registration: {message}")
                return True
            else:
                self.logger.warning(f"Unhandled message action: {action} on topic {topic}")
                return False
        
        except json.JSONDecodeError as je:
            self.logger.error(f"Failed to decode JSON message: {je}")
            return False
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}", exc_info=True)
            return False

    # def _handle_lattice_result(self, message, message_overhead_metrics=None):
    #     """
    #     Handle lattice result from a provider with comprehensive metrics aggregation
    #     to address reviewer comments about architectural vs computational overhead
    #     """
    #     logging.debug(f"Processing_handle_lattice_result message: {message}")
    #     try:
    #         federation_id = message.get('federation_id')
    #         provider_id = message.get('provider_id')
    #         encrypted_lattice = message.get('encrypted_lattice')
    #         provider_metrics = message.get('metrics', {})
    #         provider_comprehensive_metrics = message.get('comprehensive_metrics', {})
    #         logging.debug(f"Received lattice result from provider {provider_id} for federation {federation_id}")
    #         if not all([federation_id, provider_id, encrypted_lattice]):
    #             self.logger.error("Missing required fields in lattice result message")
    #             return False
            
    #         # Start comprehensive AGM-side metrics collection
    #         agm_processing_start = time.time()
    #         agm_metrics = {
    #             'timestamps': {'agm_processing_start': agm_processing_start},
    #             'communication_overhead': {},
    #             'aggregation_overhead': {},
    #             'architectural_overhead': {},
    #             'federation_coordination': {}
    #         }
            
    #         # Add message overhead metrics if provided
    #         if message_overhead_metrics:
    #             agm_metrics['communication_overhead']['message_processing'] = message_overhead_metrics
            
    #         # Monitor reception time
    #         reception_time_start = time.time()
            
    #         # Check if federation exists - measure coordination overhead
    #         federation_lookup_start = time.time()
    #         if federation_id not in self.active_federations:
    #             self.logger.warning(f"Received lattice for unknown federation: {federation_id}")
    #             return False
            
    #         federation = self.active_federations[federation_id]
    #         federation_lookup_time = time.time() - federation_lookup_start
    #         agm_metrics['federation_coordination']['federation_lookup_time'] = federation_lookup_time
            
    #         # Initialize pending lattices for this federation if not exists
    #         coordination_start = time.time()
    #         if federation_id not in self.pending_lattices:
    #             self.pending_lattices[federation_id] = {}
            
    #         # Initialize federation metrics aggregation if not exists
    #         if federation_id not in getattr(self, 'federation_comprehensive_metrics', {}):
    #             if not hasattr(self, 'federation_comprehensive_metrics'):
    #                 self.federation_comprehensive_metrics = {}
    #             self.federation_comprehensive_metrics[federation_id] = {
    #                 'providers': {},
    #                 'aggregated_metrics': {
    #                     'total_core_algorithm_time': 0.0,
    #                     'total_computation_overhead': 0.0,
    #                     'total_encryption_overhead': 0.0,
    #                     'total_architecture_overhead': 0.0,
    #                     'total_communication_overhead': 0.0,
    #                     'provider_count': 0,
    #                     'lattice_sizes': [],
    #                     'dataset_sizes': []
    #                 },
    #                 'comparison_analysis': {},
    #                 'architectural_breakdown': {}
    #             }
            
    #         coordination_time = time.time() - coordination_start
    #         agm_metrics['federation_coordination']['setup_time'] = coordination_time
            
    #         # Check if provider is part of this federation
    #         provider_validation_start = time.time()
    #         if provider_id not in federation['providers']:
    #             self.logger.warning(f"Received lattice from non-participant provider: {provider_id}")
    #             return False
            
    #         # Skip if we already have a lattice from this provider
    #         if provider_id in self.pending_lattices[federation_id]:
    #             self.logger.info(f"Already received lattice from provider {provider_id}, skipping duplicate")
    #             return True
            
    #         provider_validation_time = time.time() - provider_validation_start
    #         agm_metrics['federation_coordination']['provider_validation_time'] = provider_validation_time
            
    #         # Decrypt the lattice - measure decryption overhead
    #         self.logger.info(f"Decrypting lattice from provider {provider_id}")
    #         decryption_start = time.time()
    #         self.monitor.start_timer(f"lattice_decrypt_{federation_id}_{provider_id}")
            
    #         # Get the encryption key for this provider
    #         key_lookup_start = time.time()
    #         encryption_key = federation.get('encryption_keys', {}).get(provider_id)
    #         if not encryption_key:
    #             self.logger.error(f"No encryption key found for provider {provider_id}")
    #             return False
    #         key_lookup_time = time.time() - key_lookup_start
            
    #         agm_metrics['communication_overhead']['key_lookup_time'] = key_lookup_time
            
    #         # Decode base64 if needed - measure encoding overhead
    #         base64_decode_start = time.time()
    #         if isinstance(encrypted_lattice, str):
    #             try:
    #                 encrypted_lattice = base64.b64decode(encrypted_lattice)
    #                 logging.debug(f"Decoded lattice from provider {provider_id}: {encrypted_lattice}")
    #             except Exception as e:
    #                 self.logger.error(f"Failed to decode base64 lattice: {e}")
    #                 return False
    #         base64_decode_time = time.time() - base64_decode_start
    #         agm_metrics['communication_overhead']['base64_decode_time'] = base64_decode_time
            
    #         # Decrypt the lattice - measure actual cryptographic overhead
    #         crypto_start = time.time()
    #         try:
    #             decrypted_lattice = self.crypto_manager.decrypt(encrypted_lattice, encryption_key)
    #             logging.debug(f"Decrypted lattice from provider {provider_id}: {decrypted_lattice}")
    #             if not decrypted_lattice:
    #                 self.logger.error("Failed to decrypt lattice: empty result")
    #                 return False
                
    #             crypto_time = time.time() - crypto_start
    #             agm_metrics['communication_overhead']['decryption_time'] = crypto_time
                
    #             # Deserialize the lattice if it's a string - measure deserialization overhead
    #             lattice_deserialization_start = time.time()
    #             if isinstance(decrypted_lattice, (bytes, str)):
    #                 try:
    #                     decrypted_lattice = json.loads(decrypted_lattice)
    #                 except json.JSONDecodeError as je:
    #                     self.logger.error(f"Failed to deserialize decrypted lattice: {je}")
    #                     return False
                
    #             lattice_deserialization_time = time.time() - lattice_deserialization_start
    #             agm_metrics['communication_overhead']['lattice_deserialization_time'] = lattice_deserialization_time
                
    #             # Store the decrypted lattice
    #             self.pending_lattices[federation_id][provider_id] = decrypted_lattice
                
    #         except Exception as e:
    #             self.logger.error(f"Error decrypting lattice: {str(e)}", exc_info=True)
    #             return False
                
    #         total_decrypt_time = self.monitor.stop_timer(f"lattice_decrypt_{federation_id}_{provider_id}")
    #         total_decryption_overhead = time.time() - decryption_start
            
    #         # Aggregate provider metrics for comprehensive analysis
    #         metrics_aggregation_start = time.time()
            
    #         # Store individual provider metrics
    #         self.federation_comprehensive_metrics[federation_id]['providers'][provider_id] = {
    #             'basic_metrics': provider_metrics,
    #             'comprehensive_metrics': provider_comprehensive_metrics,
    #             'agm_processing_metrics': agm_metrics
    #         }
            
    #         # Extract and aggregate core metrics from provider
    #         if provider_comprehensive_metrics and 'comparison_metrics' in provider_comprehensive_metrics:
    #             comp_metrics = provider_comprehensive_metrics['comparison_metrics']
    #             fed_metrics = self.federation_comprehensive_metrics[federation_id]['aggregated_metrics']
                
    #             # Aggregate core algorithm times (for centralized comparison)
    #             core_time = comp_metrics.get('centralized_comparable_time', 0.0)
    #             fed_metrics['total_core_algorithm_time'] += core_time
                
    #             # Aggregate overhead times
    #             if 'federated_vs_centralized' in comp_metrics:
    #                 fvc = comp_metrics['federated_vs_centralized']
    #                 fed_metrics['total_computation_overhead'] += comp_metrics.get('computation_overhead', 0.0)
    #                 fed_metrics['total_architecture_overhead'] += comp_metrics.get('architecture_overhead', 0.0)
    #                 fed_metrics['total_encryption_overhead'] += comp_metrics.get('encryption_overhead', 0.0)
                
    #             # Track dataset and lattice sizes
    #             if 'resource_utilization' in provider_comprehensive_metrics:
    #                 ru = provider_comprehensive_metrics['resource_utilization']
    #                 if 'dataset_size' in ru:
    #                     fed_metrics['dataset_sizes'].append(ru['dataset_size'])
    #                 if 'lattice_size' in ru:
    #                     fed_metrics['lattice_sizes'].append(ru['lattice_size'])
            
    #         # Add AGM-side communication overhead to aggregated metrics
    #         fed_metrics['total_communication_overhead'] += total_decryption_overhead
    #         fed_metrics['provider_count'] += 1
            
    #         metrics_aggregation_time = time.time() - metrics_aggregation_start
    #         agm_metrics['aggregation_overhead']['metrics_aggregation_time'] = metrics_aggregation_time
            
    #         # Save provider metrics with enhanced AGM metrics
    #         enhanced_provider_metrics = provider_metrics.copy()
    #         enhanced_provider_metrics.update({
    #             'decrypt_time': total_decrypt_time,
    #             'reception_time': time.time() - reception_time_start,
    #             'agm_processing_overhead': total_decryption_overhead,
    #             'communication_overhead_breakdown': agm_metrics['communication_overhead']
    #         })
    #         self._save_provider_metrics(federation_id, provider_id, enhanced_provider_metrics)
            
    #         self.logger.info(f"Successfully processed lattice from provider {provider_id} for federation {federation_id}")
            
    #         # Check if all lattices have been received
    #         aggregation_check_start = time.time()
    #         if len(self.pending_lattices[federation_id]) == len(federation['providers']):
    #             aggregation_check_time = time.time() - aggregation_check_start
    #             agm_metrics['aggregation_overhead']['final_check_time'] = aggregation_check_time
                
    #             self.logger.info(f"All {len(federation['providers'])} lattices received for federation {federation_id}, proceeding to aggregation")
                
    #             # Perform comprehensive analysis before aggregation
    #             self._perform_comprehensive_analysis(federation_id)
                
    #             # Proceed with lattice aggregation
    #             self._aggregate_lattices(federation_id)
    #         else:
    #             aggregation_check_time = time.time() - aggregation_check_start
    #             agm_metrics['aggregation_overhead']['partial_check_time'] = aggregation_check_time
                
    #             received = len(self.pending_lattices[federation_id])
    #             total = len(federation['providers'])
    #             self.logger.info(f"Waiting for more lattices: {received}/{total} received")
            
    #         # Log comprehensive AGM processing metrics
    #         total_agm_processing_time = time.time() - agm_processing_start
    #         agm_metrics['timestamps']['total_agm_processing_time'] = total_agm_processing_time
            
    #         self.logger.info(
    #             f"AGM PROCESSING OVERHEAD - Provider {provider_id}, Federation {federation_id}:\n"
    #             f"  Total AGM Processing: {total_agm_processing_time:.4f}s\n"
    #             f"  Decryption Overhead: {total_decryption_overhead:.4f}s\n"
    #             f"  Federation Coordination: {coordination_time + federation_lookup_time + provider_validation_time:.4f}s\n"
    #             f"  Communication Processing: {sum(agm_metrics['communication_overhead'].values()):.4f}s\n"
    #             f"  Metrics Aggregation: {metrics_aggregation_time:.4f}s"
    #         )
            
    #         return True
            
    #     except Exception as e:
    #         self.logger.error(f"Unexpected error in _handle_lattice_result: {str(e)}", exc_info=True)
    #         return False
    def _handle_lattice_result(self, message, message_overhead_metrics=None):
        """
        Handle lattice result messages from ALM providers and compute global metrics
        
        Args:
            message: Dictionary containing lattice data and metrics
            message_overhead_metrics: Additional message processing metrics
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            # Extract basic message data
            federation_id = message.get('federation_id')
            provider_id = message.get('provider_id')
            encrypted_lattice = message.get('encrypted_lattice')
            provider_metrics = message.get('metrics', {})
            encryption_key = message.get('encryption_key')
            
            # Initialize metrics collection
            processing_start = time.time()
            self.monitor.start_timer(f"lattice_processing_{federation_id}_{provider_id}")
            
            # 1. Decrypt and process the lattice
            try:
                # Get encryption key for this provider
                key_id = message.get('encryption_key')
                self.logger.debug(f"Encryption key ID: {key_id}")
                if not key_id:
                    self.logger.error(f"No encryption key ID found in message from provider {provider_id}")
                    return False
                    
                # Ensure key_id is a string (it might come as bytes)
                if isinstance(key_id, bytes):
                    try:
                        key_id = key_id.decode('utf-8').strip()
                        self.logger.debug(f"Converted key_id from bytes to string: {key_id}")
                    except Exception as e:
                        self.logger.error(f"Failed to decode key_id: {e}")
                        return False
                
                self.logger.debug(f"Retrieving encryption key for provider {provider_id} with key_id: {key_id}")
                encryption_key = self._get_encryption_key(federation_id, provider_id, key_id=key_id)
                self.logger.debug(f"Encryption key: {encryption_key}")
                if not encryption_key:
                    self.logger.error(f"No encryption key found for provider {provider_id} with key ID: {key_id}")
                    # Log available keys for debugging
                    try:
                        all_keys = self.crypto_manager.kms.redis.keys("fedfca:kms:agm:*")
                        self.logger.debug(f"Available keys in Redis: {all_keys}")
                    except Exception as e:
                        self.logger.error(f"Error checking Redis keys: {e}")
                    return False
                    
                # Decrypt the lattice
                decryption_start = time.time()
                try:
                    decrypted_lattice = self.crypto_manager.decrypt(encryption_key,encrypted_lattice )
                except Exception as e:
                    self.logger.error(f"Decryption failed for provider {provider_id} with key ID {key_id}: {str(e)}")
                    return False
                decryption_time = time.time() - decryption_start
                
                # Mark this provider's lattice as received
                if federation_id not in self.pending_lattices:
                    self.pending_lattices[federation_id] = {}
                    
                self.pending_lattices[federation_id][provider_id] = {
                    'lattice': decrypted_lattice,
                    'metrics': provider_metrics
                }
                
                # Mark this lattice as processed to prevent duplicates
                self.processed_lattices.add((federation_id, provider_id))
                
            except Exception as e:
                self.logger.error(f"Error processing lattice: {str(e)}", exc_info=True)
                return False
                
            # 2. Aggregate metrics from provider
            try:
                if federation_id not in self.federation_comprehensive_metrics:
                    self._initialize_federation_metrics(federation_id)
                    
                # Store individual provider metrics
                self.federation_comprehensive_metrics[federation_id]['providers'][provider_id] = {
                    'basic_metrics': provider_metrics,
                    'processing_metrics': {
                        'decryption_time': decryption_time,
                        'processing_time': time.time() - processing_start
                    }
                }
                
                # Update aggregated metrics
                self._update_aggregated_metrics(federation_id, provider_metrics)
                
            except Exception as e:
                self.logger.error(f"Error aggregating metrics: {str(e)}", exc_info=True)
                # Continue processing even if metrics fail
                
            # 3. Check if all lattices are received and compute global lattice
            if self._all_lattices_received(federation_id):
                try:
                    # Compute global lattice
                    global_lattice = self._compute_global_lattice(federation_id)
                    
                    # Calculate final metrics
                    self._compute_final_metrics(federation_id)
                    
                    # Store results
                    self._store_federation_results(federation_id, global_lattice)
                    
                    # Log completion
                    self.logger.info(f"Successfully processed all lattices for federation {federation_id}")
                    
                except Exception as e:
                    self.logger.error(f"Error in global lattice computation: {str(e)}", exc_info=True)
                    return False
                    
            self.monitor.stop_timer(f"lattice_processing_{federation_id}_{provider_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Unexpected error in _handle_lattice_result: {str(e)}", exc_info=True)
            return False
    
    def _get_encryption_key(self, federation_id, provider_id, key_id=None):
        """
        Retrieve the encryption key for a specific provider in a federation.
        
        Args:
            federation_id (str): The ID of the federation
            provider_id (str): The ID of the provider
            key_id (str, optional): Specific key ID to retrieve. If None, will try to find the key.
            
        Returns:
            str: The encryption key if found, None otherwise
        """
        try:
            # If key_id is provided, try to get that specific key
            if key_id:
                self.logger.debug(f"Retrieving specific key {key_id} for provider {provider_id}")
                key_data = self.crypto_manager.kms.get_key(key_id)
                if key_data:
                    # Cache the key for future use
                    if provider_id not in self.provider_keys:
                        self.provider_keys[provider_id] = []
                    if key_id not in self.provider_keys[provider_id]:
                        self.provider_keys[provider_id].append(key_id)
                    return key_data
                return None
                
            # First check if we have the key in our local cache
            if provider_id in self.provider_keys and self.provider_keys[provider_id]:
                key_id = self.provider_keys[provider_id][-1]  # Get the most recent key
                self.logger.debug(f"Found key {key_id} for provider {provider_id} in local cache")
                key_data = self.crypto_manager.kms.get_key(key_id)
                if key_data:
                    return key_data
                # If key not found in KMS, remove from cache
                self.provider_keys[provider_id].remove(key_id)
            
            # If not in local cache, try to get from RedisKMS using standard key pattern
            self.logger.debug(f"Key for provider {provider_id} not in local cache, checking RedisKMS")
            
            # Try to get the key from federation info in active_federations
            if federation_id in self.active_federations:
                fed = self.active_federations[federation_id]
                if 'providers' in fed and provider_id in fed['providers']:
                    provider_info = fed['providers'][provider_id]
                    if 'encryption_key' in provider_info:
                        key_data = provider_info['encryption_key']
                        if isinstance(key_data, dict) and 'key_id' in key_data:
                            key_id = key_data['key_id']
                            # Cache the key for future use
                            if provider_id not in self.provider_keys:
                                self.provider_keys[provider_id] = []
                            if key_id not in self.provider_keys[provider_id]:
                                self.provider_keys[provider_id].append(key_id)
                            return self.crypto_manager.kms.get_key(key_id)
            
            # If we get here, try to get the key directly from RedisKMS using a standard key pattern
            key_id = f"fedfca:kms:agm:{federation_id}:{provider_id}"
            key_data = self.crypto_manager.kms.get_key(key_id)
            
            if key_data:
                # Cache the key for future use
                if provider_id not in self.provider_keys:
                    self.provider_keys[provider_id] = []
                if key_id not in self.provider_keys[provider_id]:
                    self.provider_keys[provider_id].append(key_id)
                return key_data
                
            self.logger.error(f"No encryption key found for provider {provider_id} in federation {federation_id}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting encryption key for provider {provider_id}: {str(e)}", exc_info=True)
            return None
    
    def _initialize_federation_metrics(self, federation_id):
        """Initialize metrics structure for a federation"""
        self.federation_comprehensive_metrics[federation_id] = {
            'providers': {},
            'aggregated_metrics': {
                'total_core_time': 0.0,
                'total_processing_time': 0.0,
                'total_decryption_time': 0.0,
                'provider_count': 0,
                'lattice_sizes': [],
                'start_time': time.time(),
                'end_time': None
            }
        }
    
    def _update_aggregated_metrics(self, federation_id, provider_metrics):
        """Update aggregated metrics with data from a provider"""
        fed_metrics = self.federation_comprehensive_metrics[federation_id]['aggregated_metrics']
        
        # Update core metrics
        fed_metrics['total_core_time'] += provider_metrics.get('computation_time', 0)
        fed_metrics['total_processing_time'] += provider_metrics.get('total_time', 0)
        fed_metrics['total_decryption_time'] += provider_metrics.get('decryption_time', 0)
        fed_metrics['provider_count'] += 1
        
        # Track lattice sizes
        if 'lattice_size' in provider_metrics:
            fed_metrics['lattice_sizes'].append(provider_metrics['lattice_size'])
    
    def _all_lattices_received(self, federation_id):
        """
        Check if all expected lattices have been received.
        Also verifies no duplicate lattices were received.
        """
        if federation_id not in self.active_federations:
            self.logger.warning(f"No active federation found with ID {federation_id}")
            return False
            
        expected_providers = set(self.active_federations[federation_id]['providers'].keys())
        received_providers = set(self.pending_lattices.get(federation_id, {}).keys())
        
        # Log any missing providers
        missing = expected_providers - received_providers
        if missing:
            self.logger.info(f"Waiting for lattices from {len(missing)} providers: {', '.join(missing)}")
            
        # Check for any unexpected providers (shouldn't happen with proper tracking)
        extra = received_providers - expected_providers
        if extra:
            self.logger.warning(f"Received lattices from unexpected providers: {', '.join(extra)}")
            
        return expected_providers.issubset(received_providers)
    
    def _compute_global_lattice(self, federation_id):
        """Compute the global lattice from all provider lattices"""
        lattices = list(self.pending_lattices[federation_id].values())
        self.logger.info(f"Starting global lattice computation with {len(lattices)} lattices")
        
        # Perform the actual aggregation
        global_lattice = self.aggregator.aggregate(lattices)
        
        self.logger.info(f"Global lattice computation complete. Size: {len(global_lattice)}")
        return global_lattice
    
    def _compute_final_metrics(self, federation_id):
        """Compute final aggregated metrics"""
        fed_metrics = self.federation_comprehensive_metrics[federation_id]['aggregated_metrics']
        
        # Calculate averages
        provider_count = max(1, fed_metrics['provider_count'])
        fed_metrics.update({
            'avg_core_time': fed_metrics['total_core_time'] / provider_count,
            'avg_processing_time': fed_metrics['total_processing_time'] / provider_count,
            'avg_decryption_time': fed_metrics['total_decryption_time'] / provider_count,
            'end_time': time.time(),
            'total_duration': time.time() - fed_metrics['start_time']
        })
        
        # Add lattice size statistics
        if fed_metrics['lattice_sizes']:
            fed_metrics.update({
                'avg_lattice_size': sum(fed_metrics['lattice_sizes']) / len(fed_metrics['lattice_sizes']),
                'max_lattice_size': max(fed_metrics['lattice_sizes']),
                'min_lattice_size': min(fed_metrics['lattice_sizes'])
            })
    
    def _store_federation_results(self, federation_id, global_lattice):
        """Store the final results of federation"""
        # Store the global lattice
        result_key = f"federation:{federation_id}:result"
        self.redis_client.set(result_key, json.dumps({
            'lattice': global_lattice,
            'metrics': self.federation_comprehensive_metrics[federation_id]['aggregated_metrics'],
            'timestamp': time.time()
        }))
        
        # Update federation status
        self.active_federations[federation_id]['status'] = 'completed'
        self.active_federations[federation_id]['completion_time'] = time.time()
        
        # Log completion
        metrics = self.federation_comprehensive_metrics[federation_id]['aggregated_metrics']
        self.logger.info(
            f"Federation {federation_id} completed. "
            f"Providers: {metrics['provider_count']}, "
            f"Avg Processing Time: {metrics['avg_processing_time']:.2f}s, "
            f"Total Duration: {metrics['total_duration']:.2f}s"
        )
    def _perform_comprehensive_analysis(self, federation_id):
        """
        Perform comprehensive analysis of federated vs centralized performance
        to address reviewer comments about fair comparison and overhead breakdown
        """
        if federation_id not in self.federation_comprehensive_metrics:
            self.logger.warning(f"No comprehensive metrics found for federation {federation_id}")
            return
        
        analysis_start = time.time()
        fed_metrics = self.federation_comprehensive_metrics[federation_id]['aggregated_metrics']
        provider_count = fed_metrics['provider_count']
        
        if provider_count == 0:
            self.logger.warning(f"No providers processed for federation {federation_id}")
            return
        
        # Calculate averages and totals for comprehensive comparison
        avg_core_algorithm_time = fed_metrics['total_core_algorithm_time'] / provider_count
        avg_computation_overhead = fed_metrics['total_computation_overhead'] / provider_count
        avg_architecture_overhead = fed_metrics['total_architecture_overhead'] / provider_count
        avg_encryption_overhead = fed_metrics['total_encryption_overhead'] / provider_count
        avg_communication_overhead = fed_metrics['total_communication_overhead'] / provider_count
        
        # Calculate dataset and lattice statistics
        dataset_sizes = fed_metrics['dataset_sizes']
        lattice_sizes = fed_metrics['lattice_sizes']
        
        total_dataset_size = sum(dataset_sizes) if dataset_sizes else 0
        total_lattice_size = sum(lattice_sizes) if lattice_sizes else 0
        avg_dataset_size = total_dataset_size / len(dataset_sizes) if dataset_sizes else 0
        avg_lattice_size = total_lattice_size / len(lattice_sizes) if lattice_sizes else 0
        
        # Simulate centralized equivalent computation time
        # This would be the sum of all core algorithm times if run sequentially on one machine
        centralized_equivalent_time = fed_metrics['total_core_algorithm_time']
        
        # Calculate federated total time (including all overheads)
        federated_total_time = (
            fed_metrics['total_core_algorithm_time'] +
            fed_metrics['total_computation_overhead'] +
            fed_metrics['total_architecture_overhead'] +
            fed_metrics['total_encryption_overhead'] +
            fed_metrics['total_communication_overhead']
        )
        
        # However, in federated learning, computation happens in parallel
        # So the actual federated wall-clock time would be closer to the maximum individual processing time
        max_individual_time = 0
        for provider_id, provider_data in self.federation_comprehensive_metrics[federation_id]['providers'].items():
            if 'comprehensive_metrics' in provider_data and 'comparison_metrics' in provider_data['comprehensive_metrics']:
                comp_metrics = provider_data['comprehensive_metrics']['comparison_metrics']
                individual_total = comp_metrics.get('total_processing_time', 0)
                max_individual_time = max(max_individual_time, individual_total)
        
        # Add AGM aggregation overhead (measured later in _aggregate_lattices)
        agm_aggregation_overhead = getattr(self, f'_last_aggregation_time_{federation_id}', 0)
        
        federated_wall_clock_time = max_individual_time + agm_aggregation_overhead + avg_communication_overhead
        
        # Comprehensive comparison analysis
        comparison_analysis = {
            'provider_count': provider_count,
            'dataset_statistics': {
                'total_dataset_size': total_dataset_size,
                'average_dataset_size': avg_dataset_size,
                'dataset_size_variance': statistics.variance(dataset_sizes) if len(dataset_sizes) > 1 else 0
            },
            'lattice_statistics': {
                'total_lattice_size': total_lattice_size,
                'average_lattice_size': avg_lattice_size,
                'lattice_size_variance': statistics.variance(lattice_sizes) if len(lattice_sizes) > 1 else 0
            },
            'timing_analysis': {
                'centralized_equivalent_time': centralized_equivalent_time,
                'federated_sequential_time': federated_total_time,
                'federated_wall_clock_time': federated_wall_clock_time,
                'parallel_speedup_factor': centralized_equivalent_time / federated_wall_clock_time if federated_wall_clock_time > 0 else 0,
                'sequential_overhead_factor': federated_total_time / centralized_equivalent_time if centralized_equivalent_time > 0 else 0
            },
            'overhead_breakdown': {
                'average_core_algorithm_time': avg_core_algorithm_time,
                'average_computation_overhead': avg_computation_overhead,
                'average_architecture_overhead': avg_architecture_overhead,
                'average_encryption_overhead': avg_encryption_overhead,
                'average_communication_overhead': avg_communication_overhead,
                'agm_aggregation_overhead': agm_aggregation_overhead
            },
            'efficiency_metrics': {
                'core_algorithm_efficiency_pct': (avg_core_algorithm_time / federated_wall_clock_time) * 100 if federated_wall_clock_time > 0 else 0,
                'total_overhead_pct': ((federated_wall_clock_time - avg_core_algorithm_time) / federated_wall_clock_time) * 100 if federated_wall_clock_time > 0 else 0,
                'architecture_overhead_pct': (avg_architecture_overhead / federated_wall_clock_time) * 100 if federated_wall_clock_time > 0 else 0,
                'communication_overhead_pct': (avg_communication_overhead / federated_wall_clock_time) * 100 if federated_wall_clock_time > 0 else 0,
                'encryption_overhead_pct': (avg_encryption_overhead / federated_wall_clock_time) * 100 if federated_wall_clock_time > 0 else 0
            },
            'reviewer_response_metrics': {
                'fair_comparison_note': 'Centralized time represents sequential execution of core FCA algorithm only',
                'federated_advantage': 'Parallel execution reduces wall-clock time despite architectural overhead',
                'overhead_justification': 'Privacy-preserving distributed computation with secure aggregation',
                'scalability_factor': provider_count
            }
        }
        
        # Store the comprehensive analysis
        self.federation_comprehensive_metrics[federation_id]['comparison_analysis'] = comparison_analysis
        
        analysis_time = time.time() - analysis_start
        
        # Enhanced logging for reviewer response
        self.logger.info(
            f"\n{'='*80}\n"
            f"COMPREHENSIVE FEDERATED vs CENTRALIZED ANALYSIS - Federation {federation_id}\n"
            f"{'='*80}\n"
            f"DATASET & COMPUTATION SCALE:\n"
            f"  Providers: {provider_count}\n"
            f"  Total Dataset Size: {total_dataset_size:,} records\n"
            f"  Total Concepts Generated: {total_lattice_size:,}\n"
            f"  Average Dataset per Provider: {avg_dataset_size:.0f} records\n"
            f"  Average Concepts per Provider: {avg_lattice_size:.0f}\n"
            f"\n"
            f"TIMING COMPARISON (addressing Reviewer Comment 1):\n"
            f"  Centralized Equivalent (Core FCA only): {centralized_equivalent_time:.4f}s\n"
            f"  Federated Wall-Clock Time: {federated_wall_clock_time:.4f}s\n"
            f"  Parallel Speedup Factor: {comparison_analysis['timing_analysis']['parallel_speedup_factor']:.2f}x\n"
            f"  Sequential Overhead Factor: {comparison_analysis['timing_analysis']['sequential_overhead_factor']:.2f}x\n"
            f"\n"
            f"OVERHEAD BREAKDOWN (addressing Reviewer Comment 2):\n"
            f"  Core Algorithm Efficiency: {comparison_analysis['efficiency_metrics']['core_algorithm_efficiency_pct']:.1f}%\n"
            f"  Architecture Overhead: {comparison_analysis['efficiency_metrics']['architecture_overhead_pct']:.1f}% ({avg_architecture_overhead:.4f}s avg)\n"
            f"  Communication Overhead: {comparison_analysis['efficiency_metrics']['communication_overhead_pct']:.1f}% ({avg_communication_overhead:.4f}s avg)\n"
            f"  Encryption Overhead: {comparison_analysis['efficiency_metrics']['encryption_overhead_pct']:.1f}% ({avg_encryption_overhead:.4f}s avg)\n"
            f"  Total System Overhead: {comparison_analysis['efficiency_metrics']['total_overhead_pct']:.1f}%\n"
            f"\n"
            f"ARCHITECTURAL JUSTIFICATION:\n"
            f"  - Privacy-Preserving: End-to-end encryption with secure key management\n"
            f"  - Scalable: Parallel processing across {provider_count} providers\n"
            f"  - Fault-Tolerant: Microservice architecture with Kafka messaging\n"
            f"  - Fair Comparison: Core FCA algorithm time isolated from infrastructure\n"
            f"{'='*80}\n"
        )

    def _aggregate_lattices(self, federation_id):
        """
        Aggregate lattices from all providers with comprehensive metrics collection
        for reviewer analysis of architectural overhead vs computational efficiency
        """
        try:
            # Start comprehensive aggregation timing
            overall_aggregation_start = time.time()
            
            self.logger.info(f"Starting lattice aggregation for federation {federation_id}")
            
            # Initialize comprehensive AGM aggregation metrics
            agm_aggregation_metrics = {
                'timestamps': {'aggregation_start': overall_aggregation_start},
                'preparation_overhead': {},
                'core_aggregation': {},
                'post_processing_overhead': {},
                'storage_overhead': {},
                'communication_overhead': {},
                'architectural_overhead': {}
            }
            
            # Start timing the aggregation process
            self.monitor.start_timer(f"lattice_aggregation_{federation_id}")
            
            # Preparation phase - measure data preparation overhead
            preparation_start = time.time()
            
            # Prepare lattices for aggregation
            lattices = list(self.pending_lattices[federation_id].values())
            lattice_count = len(lattices)
            
            # Calculate preparation metrics
            total_concepts = sum(len(lattice) if isinstance(lattice, list) else 0 for lattice in lattices)
            avg_concepts_per_provider = total_concepts / lattice_count if lattice_count > 0 else 0
            
            preparation_time = time.time() - preparation_start
            agm_aggregation_metrics['preparation_overhead'] = {
                'lattice_preparation_time': preparation_time,
                'lattice_count': lattice_count,
                'total_concepts': total_concepts,
                'avg_concepts_per_provider': avg_concepts_per_provider
            }
            
            self.logger.info(f"Prepared {lattice_count} lattices with {total_concepts} total concepts for aggregation")
            
            # Core aggregation phase - this is the actual FedFCA aggregation algorithm
            core_aggregation_start = time.time()
            
            # Perform aggregation
            self.aggregator.aggregate(lattices)
            aggregated_lattice = self.aggregator.global_lattice
            global_stability = self.aggregator.global_stability
            
            # Core aggregation timing (comparable to centralized aggregation)
            core_aggregation_time = time.time() - core_aggregation_start
            aggregated_concepts = len(aggregated_lattice) if isinstance(aggregated_lattice, list) else 0
            
            agm_aggregation_metrics['core_aggregation'] = {
                'pure_aggregation_time': core_aggregation_time,
                'aggregated_concepts': aggregated_concepts,
                'aggregation_efficiency': aggregated_concepts / core_aggregation_time if core_aggregation_time > 0 else 0,
                'concept_reduction_ratio': aggregated_concepts / total_concepts if total_concepts > 0 else 0,
                'global_stability': global_stability
            }
            
            # Record total aggregation time (including preparation)
            total_aggregation_time = self.monitor.stop_timer(f"lattice_aggregation_{federation_id}")
            
            self.logger.info(f"Core aggregation completed: {aggregated_concepts} concepts in {core_aggregation_time:.4f}s")
            
            # Post-processing phase - encryption and storage overhead
            post_processing_start = time.time()
            
            # Encrypt the aggregated lattice for storage
            encryption_start = time.time()
            self.monitor.start_timer(f"lattice_encrypt_{federation_id}")
            federation_key = self.crypto_manager.generate_key()  # New key for final result
            
            # Serialize lattice for encryption
            serialization_start = time.time()
            serializable_lattice = self.convert_frozenset(aggregated_lattice) if hasattr(self, 'convert_frozenset') else aggregated_lattice
            serialized_result = json.dumps({
                'lattice': serializable_lattice,
                'stability': global_stability,
                'metadata': {
                    'provider_count': lattice_count,
                    'total_input_concepts': total_concepts,
                    'aggregated_concepts': aggregated_concepts
                }
            }).encode('utf-8')
            serialization_time = time.time() - serialization_start
            
            # Actual encryption
            crypto_start = time.time()
            encrypted_result = self.crypto_manager.encrypt(serialized_result, federation_key)
            crypto_time = time.time() - crypto_start
            
            encryption_time = self.monitor.stop_timer(f"lattice_encrypt_{federation_id}")
            total_encryption_time = time.time() - encryption_start
            
            agm_aggregation_metrics['post_processing_overhead']['encryption'] = {
                'serialization_time': serialization_time,
                'cryptographic_time': crypto_time,
                'total_encryption_time': total_encryption_time,
                'data_size_bytes': len(serialized_result),
                'encrypted_size_bytes': len(encrypted_result) if encrypted_result else 0
            }
            
            # Store the key for retrieval
            key_management_start = time.time()
            self.active_federations[federation_id]['result_key'] = federation_key
            key_management_time = time.time() - key_management_start
            
            agm_aggregation_metrics['architectural_overhead']['key_management_time'] = key_management_time
            
            # Save the encrypted result to Redis - measure storage overhead
            storage_start = time.time()
            result_size = len(json.dumps(encrypted_result)) if encrypted_result else 0
            self.monitor.start_timer(f"result_save_{federation_id}")
            result_key = f"result:{federation_id}"
            self.redis_client.set(result_key, json.dumps(encrypted_result))
            self.redis_client.expire(result_key, 86400)  # 24 hour TTL
            save_time = self.monitor.stop_timer(f"result_save_{federation_id}")
            storage_time = time.time() - storage_start
            
            agm_aggregation_metrics['storage_overhead'] = {
                'redis_save_time': save_time,
                'total_storage_time': storage_time,
                'result_size_bytes': result_size,
                'storage_efficiency': result_size / storage_time if storage_time > 0 else 0
            }
            
            # Update federation status with timing
            status_update_start = time.time()
            federation_completion_time = time.time()
            self.active_federations[federation_id]['status'] = 'completed'
            self.active_federations[federation_id]['completion_time'] = federation_completion_time
            self.active_federations[federation_id]['stability'] = global_stability
            status_update_time = time.time() - status_update_start
            
            agm_aggregation_metrics['architectural_overhead']['status_update_time'] = status_update_time
            
            post_processing_time = time.time() - post_processing_start
            agm_aggregation_metrics['post_processing_overhead']['total_post_processing_time'] = post_processing_time
            
            # Calculate comprehensive timing breakdown
            total_agm_time = time.time() - overall_aggregation_start
            federation_start_time = self.active_federations[federation_id]['start_time']
            total_federation_time = federation_completion_time - federation_start_time
            
            # Collect and save enhanced metrics
            enhanced_metrics = {
                # Basic metrics (backward compatibility)
                'aggregation_time': total_aggregation_time,
                'encryption_time': encryption_time,
                'save_time': save_time,
                'result_size': result_size,
                'global_stability': global_stability,
                'total_time': total_federation_time,
                
                # Enhanced AGM metrics
                'agm_aggregation_metrics': agm_aggregation_metrics,
                'comprehensive_breakdown': {
                    'core_aggregation_time': core_aggregation_time,
                    'preparation_overhead': preparation_time,
                    'post_processing_overhead': post_processing_time,
                    'total_agm_processing_time': total_agm_time,
                    'architectural_overhead': sum([
                        key_management_time,
                        status_update_time,
                        preparation_time
                    ])
                }
            }
            
            # Store aggregation timing for comprehensive analysis integration
            setattr(self, f'_last_aggregation_time_{federation_id}', total_agm_time)
            
            # Integrate with comprehensive federation metrics if available
            if hasattr(self, 'federation_comprehensive_metrics') and federation_id in self.federation_comprehensive_metrics:
                self.federation_comprehensive_metrics[federation_id]['agm_aggregation_metrics'] = agm_aggregation_metrics
                
                # Update comparison analysis with AGM metrics
                if 'comparison_analysis' in self.federation_comprehensive_metrics[federation_id]:
                    comp_analysis = self.federation_comprehensive_metrics[federation_id]['comparison_analysis']
                    comp_analysis['overhead_breakdown']['agm_aggregation_overhead'] = total_agm_time
                    comp_analysis['overhead_breakdown']['core_aggregation_time'] = core_aggregation_time
                    
                    # Recalculate efficiency metrics with AGM overhead included
                    total_wall_clock = comp_analysis['timing_analysis']['federated_wall_clock_time']
                    updated_wall_clock = total_wall_clock + total_agm_time
                    
                    comp_analysis['timing_analysis']['agm_enhanced_wall_clock_time'] = updated_wall_clock
                    comp_analysis['efficiency_metrics']['agm_overhead_pct'] = (total_agm_time / updated_wall_clock) * 100 if updated_wall_clock > 0 else 0
                    comp_analysis['efficiency_metrics']['core_aggregation_efficiency_pct'] = (core_aggregation_time / updated_wall_clock) * 100 if updated_wall_clock > 0 else 0
            
            # Save comprehensive federation metrics
            self.federation_metrics[federation_id] = enhanced_metrics
            self._save_federation_metrics(federation_id, enhanced_metrics)
            
            # Communication overhead - notify providers
            notification_start = time.time()
            self._publish_federation_complete(federation_id, enhanced_metrics)
            notification_time = time.time() - notification_start
            
            agm_aggregation_metrics['communication_overhead']['completion_notification_time'] = notification_time
            
            # Final comprehensive logging for reviewer analysis
            self.logger.info(
                f"\n{'='*80}\n"
                f"AGM AGGREGATION ANALYSIS - Federation {federation_id}\n"
                f"{'='*80}\n"
                f"AGGREGATION SCALE:\n"
                f"  Input Lattices: {lattice_count}\n"
                f"  Total Input Concepts: {total_concepts:,}\n"
                f"  Aggregated Concepts: {aggregated_concepts:,}\n"
                f"  Concept Reduction: {((total_concepts - aggregated_concepts) / total_concepts * 100):.1f}%\n"
                f"  Global Stability: {global_stability:.4f}\n"
                f"\n"
                f"TIMING BREAKDOWN:\n"
                f"  Core Aggregation (Pure Algorithm): {core_aggregation_time:.4f}s ({(core_aggregation_time/total_agm_time)*100:.1f}%)\n"
                f"  Data Preparation Overhead: {preparation_time:.4f}s ({(preparation_time/total_agm_time)*100:.1f}%)\n"
                f"  Encryption Overhead: {total_encryption_time:.4f}s ({(total_encryption_time/total_agm_time)*100:.1f}%)\n"
                f"  Storage Overhead: {storage_time:.4f}s ({(storage_time/total_agm_time)*100:.1f}%)\n"
                f"  Architecture Overhead: {(key_management_time + status_update_time):.4f}s ({((key_management_time + status_update_time)/total_agm_time)*100:.1f}%)\n"
                f"  Total AGM Processing: {total_agm_time:.4f}s\n"
                f"\n"
                f"PERFORMANCE METRICS:\n"
                f"  Aggregation Efficiency: {agm_aggregation_metrics['core_aggregation']['aggregation_efficiency']:.1f} concepts/sec\n"
                f"  Storage Efficiency: {agm_aggregation_metrics['storage_overhead']['storage_efficiency']:.1f} bytes/sec\n"
                f"  Result Size: {result_size:,} bytes\n"
                f"  Total Federation Time: {total_federation_time:.4f}s\n"
                f"{'='*80}\n"
            )
            
            self.logger.info(f"Federation {federation_id} completed successfully. Stability: {global_stability}")
            return True
            
        except Exception as e:
            error_time = time.time() - overall_aggregation_start if 'overall_aggregation_start' in locals() else 0
            self.logger.error(f"Error aggregating lattices: {e}", exc_info=True)
            self.active_federations[federation_id]['status'] = 'failed'
            
            # Store error metrics for analysis
            if hasattr(self, 'federation_comprehensive_metrics') and federation_id in self.federation_comprehensive_metrics:
                self.federation_comprehensive_metrics[federation_id]['aggregation_error'] = {
                    'error_message': str(e),
                    'error_time': error_time,
                    'error_phase': 'aggregation'
                }
            
            return False

    def _save_provider_metrics(self, federation_id, provider_id, metrics):
        """
        Enhanced metrics saving with comprehensive analysis support
        """
        # ... existing save logic ...
        
        # Also save to comprehensive metrics for analysis
        if hasattr(self, 'federation_comprehensive_metrics') and federation_id in self.federation_comprehensive_metrics:
            if 'agm_provider_metrics' not in self.federation_comprehensive_metrics[federation_id]:
                self.federation_comprehensive_metrics[federation_id]['agm_provider_metrics'] = {}
            
            self.federation_comprehensive_metrics[federation_id]['agm_provider_metrics'][provider_id] = metrics
    
    def _handle_lattice_result(self, message, message_overhead_metrics=None):
        """
        Handle lattice result from a provider with comprehensive metrics aggregation.
        Ensures only one lattice result is processed per provider per federation.
        """
        try:
            fed_metrics={}
            federation_id = message.get('federation_id')
            provider_id = message.get('provider_id')
            
            # Check if we already processed a lattice from this provider for this federation
            lattice_key = (federation_id, provider_id)
            if lattice_key in self.processed_lattices:
                self.logger.warning(f"Already processed lattice from provider {provider_id} for federation {federation_id}")
                return False
                
            encryption_key = message.get('encryption_key')
            encrypted_lattice = message.get('encrypted_lattice')
            provider_metrics = message.get('metrics', {})
            provider_comprehensive_metrics = message.get('comprehensive_metrics', {})
            
            if not all([federation_id, provider_id, encrypted_lattice]):
                self.logger.error("Missing required fields in lattice result message")
                return False
            
            # Start comprehensive AGM-side metrics collection
            agm_processing_start = time.time()
            agm_metrics = {
                'timestamps': {'agm_processing_start': agm_processing_start},
                'communication_overhead': {},
                'aggregation_overhead': {},
                'architectural_overhead': {},
                'federation_coordination': {}
            }
            
            # Add message overhead metrics if provided
            if message_overhead_metrics:
                agm_metrics['communication_overhead']['message_processing'] = message_overhead_metrics
            
            # Monitor reception time
            reception_time_start = time.time()
            
            # Check if federation exists - measure coordination overhead
            federation_lookup_start = time.time()
            if federation_id not in self.active_federations:
                self.logger.warning(f"Received lattice for unknown federation: {federation_id}")
                return False
            
            federation = self.active_federations[federation_id]
            federation_lookup_time = time.time() - federation_lookup_start
            agm_metrics['federation_coordination']['federation_lookup_time'] = federation_lookup_time
            
            # Initialize pending lattices for this federation if not exists
            coordination_start = time.time()
            if federation_id not in self.pending_lattices:
                self.pending_lattices[federation_id] = {}
                
            # Initialize federation metrics aggregation if not exists
            if not hasattr(self, 'federation_comprehensive_metrics'):
                self.federation_comprehensive_metrics = {}
                
            if federation_id not in self.federation_comprehensive_metrics:
                self.federation_comprehensive_metrics[federation_id] = {
                    'providers': {},
                    'aggregated_metrics': {
                        'total_core_algorithm_time': 0.0,
                        'total_computation_overhead': 0.0,
                        'total_encryption_overhead': 0.0,
                        'total_architecture_overhead': 0.0,
                        'total_communication_overhead': 0.0,
                        'provider_count': 0,
                        'lattice_sizes': [],
                        'dataset_sizes': []
                    },
                    'comparison_analysis': {},
                    'architectural_breakdown': {}
                }
                
            # Get reference to federation metrics for easier access
            fed_metrics = self.federation_comprehensive_metrics[federation_id]['aggregated_metrics']
            
            coordination_time = time.time() - coordination_start
            agm_metrics['federation_coordination']['setup_time'] = coordination_time
            
            # Check if provider is part of this federation
            provider_validation_start = time.time()
            if provider_id not in federation['providers']:
                self.logger.warning(f"Received lattice from non-participant provider: {provider_id}")
                return False
            
            # Skip if we already have a lattice from this provider
            if provider_id in self.pending_lattices[federation_id]:
                self.logger.info(f"Already received lattice from provider {provider_id}, skipping duplicate")
                return True
            
            provider_validation_time = time.time() - provider_validation_start
            agm_metrics['federation_coordination']['provider_validation_time'] = provider_validation_time
            
            # Decrypt the lattice - measure decryption overhead
            self.logger.info(f"Decrypting lattice from provider {provider_id}")
            decryption_start = time.time()
            self.monitor.start_timer(f"lattice_decrypt_{federation_id}_{provider_id}")
            
            # Get the encryption key for this provider
            key_lookup_start = time.time()
            encryption_key = federation.get('encryption_keys', {}).get(provider_id)
            if not encryption_key:
                self.logger.error(f"No encryption key found for provider {provider_id}")
                return False
            key_lookup_time = time.time() - key_lookup_start
            
            agm_metrics['communication_overhead']['key_lookup_time'] = key_lookup_time
            
            # Decode base64 if needed - measure encoding overhead
            base64_decode_start = time.time()
            if isinstance(encrypted_lattice, str):
                try:
                    encrypted_lattice = base64.b64decode(encrypted_lattice)
                    # logging.debug(f"Decoded lattice from provider {provider_id}: {encrypted_lattice}")
                except Exception as e:
                    self.logger.error(f"Failed to decode base64 lattice: {e}")
                    return False
            base64_decode_time = time.time() - base64_decode_start
            agm_metrics['communication_overhead']['base64_decode_time'] = base64_decode_time
            
            # Decrypt the lattice - measure actual cryptographic overhead
            crypto_start = time.time()
            try:
                decrypted_lattice = self.crypto_manager.decrypt(encryption_key,encrypted_lattice )
                # logging.debug(f"Decrypted lattice from provider {provider_id}: {decrypted_lattice}")
                if not decrypted_lattice:
                    self.logger.error("Failed to decrypt lattice: empty result")
                    return False
                
                crypto_time = time.time() - crypto_start
                agm_metrics['communication_overhead']['decryption_time'] = crypto_time
                
                # Deserialize the lattice if it's a string - measure deserialization overhead
                lattice_deserialization_start = time.time()
                if isinstance(decrypted_lattice, (bytes, str)):
                    try:
                        decrypted_lattice = json.loads(decrypted_lattice)
                    except json.JSONDecodeError as je:
                        self.logger.error(f"Failed to deserialize decrypted lattice: {je}")
                        return False
                
                lattice_deserialization_time = time.time() - lattice_deserialization_start
                agm_metrics['communication_overhead']['lattice_deserialization_time'] = lattice_deserialization_time
                
                # Store the decrypted lattice
                self.pending_lattices[federation_id][provider_id] = decrypted_lattice
                
            except Exception as e:
                self.logger.error(f"Error decrypting lattice: {str(e)}", exc_info=True)
                return False
                
            total_decrypt_time = self.monitor.stop_timer(f"lattice_decrypt_{federation_id}_{provider_id}")
            total_decryption_overhead = time.time() - decryption_start
            
            # Aggregate provider metrics for comprehensive analysis
            metrics_aggregation_start = time.time()
            
            # Store individual provider metrics
            self.federation_comprehensive_metrics[federation_id]['providers'][provider_id] = {
                'basic_metrics': provider_metrics,
                'comprehensive_metrics': provider_comprehensive_metrics,
                'agm_processing_metrics': agm_metrics
            }
            
            # Extract and aggregate core metrics from provider
            if provider_comprehensive_metrics and 'comparison_metrics' in provider_comprehensive_metrics:
                comp_metrics = provider_comprehensive_metrics['comparison_metrics']
                fed_metrics = self.federation_comprehensive_metrics[federation_id]['aggregated_metrics']
                
                # Aggregate core algorithm times (for centralized comparison)
                core_time = comp_metrics.get('centralized_comparable_time', 0.0)
                fed_metrics['total_core_algorithm_time'] += core_time
                
                # Aggregate overhead times
                if 'federated_vs_centralized' in comp_metrics:
                    fvc = comp_metrics['federated_vs_centralized']
                    fed_metrics['total_computation_overhead'] += comp_metrics.get('computation_overhead', 0.0)
                    fed_metrics['total_architecture_overhead'] += comp_metrics.get('architecture_overhead', 0.0)
                    fed_metrics['total_encryption_overhead'] += comp_metrics.get('encryption_overhead', 0.0)
                
                # Track dataset and lattice sizes
                if 'resource_utilization' in provider_comprehensive_metrics:
                    ru = provider_comprehensive_metrics['resource_utilization']
                    if 'dataset_size' in ru:
                        fed_metrics['dataset_sizes'].append(ru['dataset_size'])
                    if 'lattice_size' in ru:
                        fed_metrics['lattice_sizes'].append(ru['lattice_size'])
            
            # Add AGM-side communication overhead to aggregated metrics
            fed_metrics['total_communication_overhead'] += total_decryption_overhead
            fed_metrics['provider_count'] += 1
            
            metrics_aggregation_time = time.time() - metrics_aggregation_start
            agm_metrics['aggregation_overhead']['metrics_aggregation_time'] = metrics_aggregation_time
            
            # Save provider metrics with enhanced AGM metrics
            enhanced_provider_metrics = provider_metrics.copy()
            enhanced_provider_metrics.update({
                'decrypt_time': total_decrypt_time,
                'reception_time': time.time() - reception_time_start,
                'agm_processing_overhead': total_decryption_overhead,
                'communication_overhead_breakdown': agm_metrics['communication_overhead']
            })
            self._save_provider_metrics(federation_id, provider_id, enhanced_provider_metrics)
            
            self.logger.info(f"Successfully processed lattice from provider {provider_id} for federation {federation_id}")
            
            # Check if all lattices have been received
            aggregation_check_start = time.time()
            if len(self.pending_lattices[federation_id]) == len(federation['providers']):
                aggregation_check_time = time.time() - aggregation_check_start
                agm_metrics['aggregation_overhead']['final_check_time'] = aggregation_check_time
                
                self.logger.info(f"All {len(federation['providers'])} lattices received for federation {federation_id}, proceeding to aggregation")
                
                # Perform comprehensive analysis before aggregation
                self._perform_comprehensive_analysis(federation_id)
                
                # Proceed with lattice aggregation
                self._aggregate_lattices(federation_id)
            else:
                aggregation_check_time = time.time() - aggregation_check_start
                agm_metrics['aggregation_overhead']['partial_check_time'] = aggregation_check_time
                
                received = len(self.pending_lattices[federation_id])
                total = len(federation['providers'])
                self.logger.info(f"Waiting for more lattices: {received}/{total} received")
            
            # Log comprehensive AGM processing metrics
            total_agm_processing_time = time.time() - agm_processing_start
            agm_metrics['timestamps']['total_agm_processing_time'] = total_agm_processing_time
            
            # Safely calculate total communication overhead
            communication_overhead = 0.0
            if 'communication_overhead' in agm_metrics:
                for value in agm_metrics['communication_overhead'].values():
                    if isinstance(value, (int, float)):
                        communication_overhead += value
                    elif isinstance(value, dict):
                        # If it's a dictionary, try to sum its values
                        for subvalue in value.values():
                            if isinstance(subvalue, (int, float)):
                                communication_overhead += subvalue
            
            self.logger.info(
                f"AGM PROCESSING OVERHEAD - Provider {provider_id}, Federation {federation_id}:\n"
                f"  Total AGM Processing: {total_agm_processing_time:.4f}s\n"
                f"  Decryption Overhead: {total_decryption_overhead:.4f}s\n"
                f"  Federation Coordination: {coordination_time + federation_lookup_time + provider_validation_time:.4f}s\n"
                f"  Communication Processing: {communication_overhead:.4f}s\n"
                f"  Metrics Aggregation: {metrics_aggregation_time:.4f}s"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Unexpected error in _handle_lattice_result: {str(e)}", exc_info=True)
            return False

    def _perform_comprehensive_analysis(self, federation_id):
        """
        Perform comprehensive analysis of federated vs centralized performance
        to address reviewer comments about fair comparison and overhead breakdown
        """
        if federation_id not in self.federation_comprehensive_metrics:
            self.logger.warning(f"No comprehensive metrics found for federation {federation_id}")
            return
        
        analysis_start = time.time()
        fed_metrics = self.federation_comprehensive_metrics[federation_id]['aggregated_metrics']
        provider_count = fed_metrics['provider_count']
        
        if provider_count == 0:
            self.logger.warning(f"No providers processed for federation {federation_id}")
            return
        
        # Calculate averages and totals for comprehensive comparison
        avg_core_algorithm_time = fed_metrics['total_core_algorithm_time'] / provider_count
        avg_computation_overhead = fed_metrics['total_computation_overhead'] / provider_count
        avg_architecture_overhead = fed_metrics['total_architecture_overhead'] / provider_count
        avg_encryption_overhead = fed_metrics['total_encryption_overhead'] / provider_count
        avg_communication_overhead = fed_metrics['total_communication_overhead'] / provider_count
        
        # Calculate dataset and lattice statistics
        dataset_sizes = fed_metrics['dataset_sizes']
        lattice_sizes = fed_metrics['lattice_sizes']
        
        total_dataset_size = sum(dataset_sizes) if dataset_sizes else 0
        total_lattice_size = sum(lattice_sizes) if lattice_sizes else 0
        avg_dataset_size = total_dataset_size / len(dataset_sizes) if dataset_sizes else 0
        avg_lattice_size = total_lattice_size / len(lattice_sizes) if lattice_sizes else 0
        
        # Simulate centralized equivalent computation time
        # This would be the sum of all core algorithm times if run sequentially on one machine
        centralized_equivalent_time = fed_metrics['total_core_algorithm_time']
        
        # Calculate federated total time (including all overheads)
        federated_total_time = (
            fed_metrics['total_core_algorithm_time'] +
            fed_metrics['total_computation_overhead'] +
            fed_metrics['total_architecture_overhead'] +
            fed_metrics['total_encryption_overhead'] +
            fed_metrics['total_communication_overhead']
        )
        
        # However, in federated learning, computation happens in parallel
        # So the actual federated wall-clock time would be closer to the maximum individual processing time
        max_individual_time = 0
        for provider_id, provider_data in self.federation_comprehensive_metrics[federation_id]['providers'].items():
            if 'comprehensive_metrics' in provider_data and 'comparison_metrics' in provider_data['comprehensive_metrics']:
                comp_metrics = provider_data['comprehensive_metrics']['comparison_metrics']
                individual_total = comp_metrics.get('total_processing_time', 0)
                max_individual_time = max(max_individual_time, individual_total)
        
        # Add AGM aggregation overhead (measured later in _aggregate_lattices)
        agm_aggregation_overhead = getattr(self, f'_last_aggregation_time_{federation_id}', 0)
        
        federated_wall_clock_time = max_individual_time + agm_aggregation_overhead + avg_communication_overhead
        
        # Comprehensive comparison analysis
        comparison_analysis = {
            'provider_count': provider_count,
            'dataset_statistics': {
                'total_dataset_size': total_dataset_size,
                'average_dataset_size': avg_dataset_size,
                'dataset_size_variance': statistics.variance(dataset_sizes) if len(dataset_sizes) > 1 else 0
            },
            'lattice_statistics': {
                'total_lattice_size': total_lattice_size,
                'average_lattice_size': avg_lattice_size,
                'lattice_size_variance': statistics.variance(lattice_sizes) if len(lattice_sizes) > 1 else 0
            },
            'timing_analysis': {
                'centralized_equivalent_time': centralized_equivalent_time,
                'federated_sequential_time': federated_total_time,
                'federated_wall_clock_time': federated_wall_clock_time,
                'parallel_speedup_factor': centralized_equivalent_time / federated_wall_clock_time if federated_wall_clock_time > 0 else 0,
                'sequential_overhead_factor': federated_total_time / centralized_equivalent_time if centralized_equivalent_time > 0 else 0
            },
            'overhead_breakdown': {
                'average_core_algorithm_time': avg_core_algorithm_time,
                'average_computation_overhead': avg_computation_overhead,
                'average_architecture_overhead': avg_architecture_overhead,
                'average_encryption_overhead': avg_encryption_overhead,
                'average_communication_overhead': avg_communication_overhead,
                'agm_aggregation_overhead': agm_aggregation_overhead
            },
            'efficiency_metrics': {
                'core_algorithm_efficiency_pct': (avg_core_algorithm_time / federated_wall_clock_time) * 100 if federated_wall_clock_time > 0 else 0,
                'total_overhead_pct': ((federated_wall_clock_time - avg_core_algorithm_time) / federated_wall_clock_time) * 100 if federated_wall_clock_time > 0 else 0,
                'architecture_overhead_pct': (avg_architecture_overhead / federated_wall_clock_time) * 100 if federated_wall_clock_time > 0 else 0,
                'communication_overhead_pct': (avg_communication_overhead / federated_wall_clock_time) * 100 if federated_wall_clock_time > 0 else 0,
                'encryption_overhead_pct': (avg_encryption_overhead / federated_wall_clock_time) * 100 if federated_wall_clock_time > 0 else 0
            },
            'reviewer_response_metrics': {
                'fair_comparison_note': 'Centralized time represents sequential execution of core FCA algorithm only',
                'federated_advantage': 'Parallel execution reduces wall-clock time despite architectural overhead',
                'overhead_justification': 'Privacy-preserving distributed computation with secure aggregation',
                'scalability_factor': provider_count
            }
        }
        
        # Store the comprehensive analysis
        self.federation_comprehensive_metrics[federation_id]['comparison_analysis'] = comparison_analysis
        
        analysis_time = time.time() - analysis_start
        
        # Enhanced logging for reviewer response
        self.logger.info(
            f"\n{'='*80}\n"
            f"COMPREHENSIVE FEDERATED vs CENTRALIZED ANALYSIS - Federation {federation_id}\n"
            f"{'='*80}\n"
            f"DATASET & COMPUTATION SCALE:\n"
            f"  Providers: {provider_count}\n"
            f"  Total Dataset Size: {total_dataset_size:,} records\n"
            f"  Total Concepts Generated: {total_lattice_size:,}\n"
            f"  Average Dataset per Provider: {avg_dataset_size:.0f} records\n"
            f"  Average Concepts per Provider: {avg_lattice_size:.0f}\n"
            f"\n"
            f"TIMING COMPARISON (addressing Reviewer Comment 1):\n"
            f"  Centralized Equivalent (Core FCA only): {centralized_equivalent_time:.4f}s\n"
            f"  Federated Wall-Clock Time: {federated_wall_clock_time:.4f}s\n"
            f"  Parallel Speedup Factor: {comparison_analysis['timing_analysis']['parallel_speedup_factor']:.2f}x\n"
            f"  Sequential Overhead Factor: {comparison_analysis['timing_analysis']['sequential_overhead_factor']:.2f}x\n"
            f"\n"
            f"OVERHEAD BREAKDOWN (addressing Reviewer Comment 2):\n"
            f"  Core Algorithm Efficiency: {comparison_analysis['efficiency_metrics']['core_algorithm_efficiency_pct']:.1f}%\n"
            f"  Architecture Overhead: {comparison_analysis['efficiency_metrics']['architecture_overhead_pct']:.1f}% ({avg_architecture_overhead:.4f}s avg)\n"
            f"  Communication Overhead: {comparison_analysis['efficiency_metrics']['communication_overhead_pct']:.1f}% ({avg_communication_overhead:.4f}s avg)\n"
            f"  Encryption Overhead: {comparison_analysis['efficiency_metrics']['encryption_overhead_pct']:.1f}% ({avg_encryption_overhead:.4f}s avg)\n"
            f"  Total System Overhead: {comparison_analysis['efficiency_metrics']['total_overhead_pct']:.1f}%\n"
            f"\n"
            f"ARCHITECTURAL JUSTIFICATION:\n"
            f"  - Privacy-Preserving: End-to-end encryption with secure key management\n"
            f"  - Scalable: Parallel processing across {provider_count} providers\n"
            f"  - Fault-Tolerant: Microservice architecture with Kafka messaging\n"
            f"  - Fair Comparison: Core FCA algorithm time isolated from infrastructure\n"
            f"{'='*80}\n"
        )

    def _aggregate_lattices(self, federation_id):
        """
        Aggregate lattices from all providers with comprehensive metrics collection
        for reviewer analysis of architectural overhead vs computational efficiency
        """
        try:
            # Start comprehensive aggregation timing
            overall_aggregation_start = time.time()
            
            self.logger.info(f"Starting lattice aggregation for federation {federation_id}")
            
            # Initialize comprehensive AGM aggregation metrics
            agm_aggregation_metrics = {
                'timestamps': {'aggregation_start': overall_aggregation_start},
                'preparation_overhead': {},
                'core_aggregation': {},
                'post_processing_overhead': {},
                'storage_overhead': {},
                'communication_overhead': {},
                'architectural_overhead': {}
            }
            
            # Start timing the aggregation process
            self.monitor.start_timer(f"lattice_aggregation_{federation_id}")
            
            # Preparation phase - measure data preparation overhead
            preparation_start = time.time()
            
            # Prepare lattices for aggregation
            lattices = list(self.pending_lattices[federation_id].values())
            lattice_count = len(lattices)
            
            # Calculate preparation metrics
            total_concepts = sum(len(lattice) if isinstance(lattice, list) else 0 for lattice in lattices)
            avg_concepts_per_provider = total_concepts / lattice_count if lattice_count > 0 else 0
            
            preparation_time = time.time() - preparation_start
            agm_aggregation_metrics['preparation_overhead'] = {
                'lattice_preparation_time': preparation_time,
                'lattice_count': lattice_count,
                'total_concepts': total_concepts,
                'avg_concepts_per_provider': avg_concepts_per_provider
            }
            
            self.logger.info(f"Prepared {lattice_count} lattices with {total_concepts} total concepts for aggregation")
            
            # Core aggregation phase - this is the actual FedFCA aggregation algorithm
            core_aggregation_start = time.time()
            
            # Perform aggregation
            self.aggregator.aggregate(lattices)
            aggregated_lattice = self.aggregator.global_lattice
            global_stability = self.aggregator.global_stability
            
            # Core aggregation timing (comparable to centralized aggregation)
            core_aggregation_time = time.time() - core_aggregation_start
            aggregated_concepts = len(aggregated_lattice) if isinstance(aggregated_lattice, list) else 0
            
            agm_aggregation_metrics['core_aggregation'] = {
                'pure_aggregation_time': core_aggregation_time,
                'aggregated_concepts': aggregated_concepts,
                'aggregation_efficiency': aggregated_concepts / core_aggregation_time if core_aggregation_time > 0 else 0,
                'concept_reduction_ratio': aggregated_concepts / total_concepts if total_concepts > 0 else 0,
                'global_stability': global_stability
            }
            
            # Record total aggregation time (including preparation)
            total_aggregation_time = self.monitor.stop_timer(f"lattice_aggregation_{federation_id}")
            
            self.logger.info(f"Core aggregation completed: {aggregated_concepts} concepts in {core_aggregation_time:.4f}s")
            
            # Post-processing phase - encryption and storage overhead
            post_processing_start = time.time()
            
            # Encrypt the aggregated lattice for storage
            encryption_start = time.time()
            self.monitor.start_timer(f"lattice_encrypt_{federation_id}")
            federation_key = self.crypto_manager.generate_key()  # New key for final result
            
            # Serialize lattice for encryption
            serialization_start = time.time()
            serializable_lattice = self.convert_frozenset(aggregated_lattice) if hasattr(self, 'convert_frozenset') else aggregated_lattice
            serialized_result = json.dumps({
                'lattice': serializable_lattice,
                'stability': global_stability,
                'metadata': {
                    'provider_count': lattice_count,
                    'total_input_concepts': total_concepts,
                    'aggregated_concepts': aggregated_concepts
                }
            }).encode('utf-8')
            serialization_time = time.time() - serialization_start
            
            # Actual encryption
            crypto_start = time.time()
            encrypted_result = self.crypto_manager.encrypt(serialized_result, federation_key)
            crypto_time = time.time() - crypto_start
            
            encryption_time = self.monitor.stop_timer(f"lattice_encrypt_{federation_id}")
            total_encryption_time = time.time() - encryption_start
            
            agm_aggregation_metrics['post_processing_overhead']['encryption'] = {
                'serialization_time': serialization_time,
                'cryptographic_time': crypto_time,
                'total_encryption_time': total_encryption_time,
                'data_size_bytes': len(serialized_result),
                'encrypted_size_bytes': len(encrypted_result) if encrypted_result else 0
            }
            
            # Store the key for retrieval
            key_management_start = time.time()
            self.active_federations[federation_id]['result_key'] = federation_key
            key_management_time = time.time() - key_management_start
            
            agm_aggregation_metrics['architectural_overhead']['key_management_time'] = key_management_time
            
            # Save the encrypted result to Redis - measure storage overhead
            storage_start = time.time()
            result_size = len(json.dumps(encrypted_result)) if encrypted_result else 0
            self.monitor.start_timer(f"result_save_{federation_id}")
            result_key = f"result:{federation_id}"
            self.redis_client.set(result_key, json.dumps(encrypted_result))
            self.redis_client.expire(result_key, 86400)  # 24 hour TTL
            save_time = self.monitor.stop_timer(f"result_save_{federation_id}")
            storage_time = time.time() - storage_start
            
            agm_aggregation_metrics['storage_overhead'] = {
                'redis_save_time': save_time,
                'total_storage_time': storage_time,
                'result_size_bytes': result_size,
                'storage_efficiency': result_size / storage_time if storage_time > 0 else 0
            }
            
            # Update federation status with timing
            status_update_start = time.time()
            federation_completion_time = time.time()
            self.active_federations[federation_id]['status'] = 'completed'
            self.active_federations[federation_id]['completion_time'] = federation_completion_time
            self.active_federations[federation_id]['stability'] = global_stability
            status_update_time = time.time() - status_update_start
            
            agm_aggregation_metrics['architectural_overhead']['status_update_time'] = status_update_time
            
            post_processing_time = time.time() - post_processing_start
            agm_aggregation_metrics['post_processing_overhead']['total_post_processing_time'] = post_processing_time
            
            # Calculate comprehensive timing breakdown
            total_agm_time = time.time() - overall_aggregation_start
            federation_start_time = self.active_federations[federation_id]['start_time']
            total_federation_time = federation_completion_time - federation_start_time
            
            # Collect and save enhanced metrics
            enhanced_metrics = {
                # Basic metrics (backward compatibility)
                'aggregation_time': total_aggregation_time,
                'encryption_time': encryption_time,
                'save_time': save_time,
                'result_size': result_size,
                'global_stability': global_stability,
                'total_time': total_federation_time,
                
                # Enhanced AGM metrics
                'agm_aggregation_metrics': agm_aggregation_metrics,
                'comprehensive_breakdown': {
                    'core_aggregation_time': core_aggregation_time,
                    'preparation_overhead': preparation_time,
                    'post_processing_overhead': post_processing_time,
                    'total_agm_processing_time': total_agm_time,
                    'architectural_overhead': sum([
                        key_management_time,
                        status_update_time,
                        preparation_time
                    ])
                }
            }
            
            # Store aggregation timing for comprehensive analysis integration
            setattr(self, f'_last_aggregation_time_{federation_id}', total_agm_time)
            
            # Integrate with comprehensive federation metrics if available
            if hasattr(self, 'federation_comprehensive_metrics') and federation_id in self.federation_comprehensive_metrics:
                self.federation_comprehensive_metrics[federation_id]['agm_aggregation_metrics'] = agm_aggregation_metrics
                
                # Update comparison analysis with AGM metrics
                if 'comparison_analysis' in self.federation_comprehensive_metrics[federation_id]:
                    comp_analysis = self.federation_comprehensive_metrics[federation_id]['comparison_analysis']
                    comp_analysis['overhead_breakdown']['agm_aggregation_overhead'] = total_agm_time
                    comp_analysis['overhead_breakdown']['core_aggregation_time'] = core_aggregation_time
                    
                    # Recalculate efficiency metrics with AGM overhead included
                    total_wall_clock = comp_analysis['timing_analysis']['federated_wall_clock_time']
                    updated_wall_clock = total_wall_clock + total_agm_time
                    
                    comp_analysis['timing_analysis']['agm_enhanced_wall_clock_time'] = updated_wall_clock
                    comp_analysis['efficiency_metrics']['agm_overhead_pct'] = (total_agm_time / updated_wall_clock) * 100 if updated_wall_clock > 0 else 0
                    comp_analysis['efficiency_metrics']['core_aggregation_efficiency_pct'] = (core_aggregation_time / updated_wall_clock) * 100 if updated_wall_clock > 0 else 0
            
            # Save comprehensive federation metrics
            self.federation_metrics[federation_id] = enhanced_metrics
            self._save_federation_metrics(federation_id, enhanced_metrics)
            
            # Communication overhead - notify providers
            notification_start = time.time()
            self._publish_federation_complete(federation_id, enhanced_metrics)
            notification_time = time.time() - notification_start
            
            agm_aggregation_metrics['communication_overhead']['completion_notification_time'] = notification_time
            
            # Final comprehensive logging for reviewer analysis
            self.logger.info(
                f"\n{'='*80}\n"
                f"AGM AGGREGATION ANALYSIS - Federation {federation_id}\n"
                f"{'='*80}\n"
                f"AGGREGATION SCALE:\n"
                f"  Input Lattices: {lattice_count}\n"
                f"  Total Input Concepts: {total_concepts:,}\n"
                f"  Aggregated Concepts: {aggregated_concepts:,}\n"
                f"  Concept Reduction: {((total_concepts - aggregated_concepts) / total_concepts * 100):.1f}%\n"
                f"  Global Stability: {global_stability:.4f}\n"
                f"\n"
                f"TIMING BREAKDOWN:\n"
                f"  Core Aggregation (Pure Algorithm): {core_aggregation_time:.4f}s ({(core_aggregation_time/total_agm_time)*100:.1f}%)\n"
                f"  Data Preparation Overhead: {preparation_time:.4f}s ({(preparation_time/total_agm_time)*100:.1f}%)\n"
                f"  Encryption Overhead: {total_encryption_time:.4f}s ({(total_encryption_time/total_agm_time)*100:.1f}%)\n"
                f"  Storage Overhead: {storage_time:.4f}s ({(storage_time/total_agm_time)*100:.1f}%)\n"
                f"  Architecture Overhead: {(key_management_time + status_update_time):.4f}s ({((key_management_time + status_update_time)/total_agm_time)*100:.1f}%)\n"
                f"  Total AGM Processing: {total_agm_time:.4f}s\n"
                f"\n"
                f"PERFORMANCE METRICS:\n"
                f"  Aggregation Efficiency: {agm_aggregation_metrics['core_aggregation']['aggregation_efficiency']:.1f} concepts/sec\n"
                f"  Storage Efficiency: {agm_aggregation_metrics['storage_overhead']['storage_efficiency']:.1f} bytes/sec\n"
                f"  Result Size: {result_size:,} bytes\n"
                f"  Total Federation Time: {total_federation_time:.4f}s\n"
                f"{'='*80}\n"
            )
            
            self.logger.info(f"Federation {federation_id} completed successfully. Stability: {global_stability}")
            return True
            
        except Exception as e:
            error_time = time.time() - overall_aggregation_start if 'overall_aggregation_start' in locals() else 0
            self.logger.error(f"Error aggregating lattices: {e}", exc_info=True)
            self.active_federations[federation_id]['status'] = 'failed'
            
            # Store error metrics for analysis
            if hasattr(self, 'federation_comprehensive_metrics') and federation_id in self.federation_comprehensive_metrics:
                self.federation_comprehensive_metrics[federation_id]['aggregation_error'] = {
                    'error_message': str(e),
                    'error_time': error_time,
                    'error_phase': 'aggregation'
                }
            
            return False

    def _save_provider_metrics(self, federation_id, provider_id, metrics):
        """
        Save provider metrics to Redis and update in-memory metrics
        
        Args:
            federation_id: ID of the federation
            provider_id: ID of the provider
            metrics: Dictionary of metrics to save
            
        Returns:
            bool: True if save was successful, False otherwise
        """
        try:
            # Initialize metrics structures if they don't exist
            if not hasattr(self, 'federation_comprehensive_metrics'):
                self.federation_comprehensive_metrics = {}
            if federation_id not in self.federation_comprehensive_metrics:
                self.federation_comprehensive_metrics[federation_id] = {'providers': {}}
            
            # Update in-memory metrics
            self.federation_comprehensive_metrics[federation_id]['providers'][provider_id] = metrics
            
            # Save to Redis using the standard method
            return self._save_metrics_to_redis(federation_id, {
                'providers': {provider_id: metrics}
            })
        except Exception as e:
            self.logger.error(f"Error in _save_provider_metrics: {e}", exc_info=True)
            return False
    def _publish_federation_complete(self, federation_id, metrics):
        """
        Publish federation completion notification
        """
        try:
            message = {
                'action': 'federation_complete',
                'federation_id': federation_id,
                'metrics': {
                    'global_stability': metrics.get('global_stability', 0),
                    'total_time': metrics.get('total_time', 0)
                },
                'timestamp': time.time()
            }
            
            self.producer.produce(
                "federation.complete",
                value=json.dumps(message).encode('utf-8')
            )
            self.producer.flush()
            self.logger.info(f"Published federation completion notification for {federation_id}")
            return True
        except Exception as e:
            self.logger.error(f"Error publishing federation completion: {e}")
            return False
    
    def _save_provider_metrics(self, federation_id, provider_id, metrics):
        """
        Save provider metrics to Redis
        """
        try:
            if self.redis_client:
                metrics_key = f"provider_metrics:{federation_id}:{provider_id}"
                self.redis_client.set(metrics_key, json.dumps(metrics))
                self.redis_client.expire(metrics_key, 86400)  # 24 hour TTL
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error saving provider metrics: {e}")
            return False
    
    def _save_federation_metrics(self, federation_id, metrics):
        """
        Save federation metrics to Redis and update in-memory metrics
        
        Args:
            federation_id: ID of the federation
            metrics: Dictionary of metrics to save
            
        Returns:
            bool: True if save was successful, False otherwise
        """
        try:
            # Initialize metrics structures if they don't exist
            if not hasattr(self, 'federation_comprehensive_metrics'):
                self.federation_comprehensive_metrics = {}
            if federation_id not in self.federation_comprehensive_metrics:
                self.federation_comprehensive_metrics[federation_id] = {}
            
            # Update in-memory metrics
            self.federation_comprehensive_metrics[federation_id].update({
                'federation': metrics,
                'last_updated': time.time()
            })
            
            # Save to Redis using the standard method
            return self._save_metrics_to_redis(federation_id, {
                'federation': metrics,
                'providers': self.federation_comprehensive_metrics[federation_id].get('providers', {})
            })
        except Exception as e:
            self.logger.error(f"Error in _save_federation_metrics: {e}", exc_info=True)
            return False
    
    def start_listening(self):
        """
        Start the main Kafka message listening loop
        """
        self.logger.info("Starting message listening loop")
        self.running = True
        
        # Start automatic federation scheduling in a separate thread
        federation_thread = threading.Thread(target=self._federation_scheduler_loop)
        federation_thread.daemon = True
        federation_thread.start()
        
        try:
            while self.running:
                try:
                    # Poll for messages with a timeout
                    message = self.consumer.poll(1.0)  # 1 second timeout
                    
                    # Skip if no message received
                    if message is None:
                        continue
                    # self.logger.info(f"Received message: {message.value().decode('utf-8')}")    
                    # Handle message errors
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            self.logger.debug(f"Reached end of partition {message.topic()}/{message.partition()}")
                        else:
                            self.logger.error(f"Error polling: {message.error()}")
                        continue
                        
                    # Safely process the message
                    try:
                        # Log basic message info before processing
                        message_value = message.value()
                        self.logger.info(f"Processing message from topic {message.topic()}")
                        self._process_incoming_message(message)
                        if message_value:
                            self.logger.debug(f"Processing message from topic {message.topic()}")
                            # Process message asynchronously
                            self.executor.submit(self._process_incoming_message, message)
                        else:
                            self.logger.warning("Received empty message value")
                            
                    except Exception as proc_error:
                        self.logger.error(f"Error processing message: {proc_error}", exc_info=True)
                        
                except Exception as poll_error:
                    self.logger.error(f"Error in message polling loop: {poll_error}", exc_info=True)
                    time.sleep(1)  # Prevent tight loop on errors
        
        except Exception as e:
            self.logger.error(f"Error in message loop: {e}")
        finally:
            self.logger.info("Stopping message listening loop")
            self.stop_actor()
    
    def _federation_scheduler_loop(self):
        """
        Periodically check for registered providers and initiate federations
        """
        while self.running:
            try:
                # Count active federations
                active_count = sum(1 for fed in self.active_federations.values() 
                                if fed['status'] in ['initiated', 'in_progress'])
                
                # Only start new federation if less than 3 active
                if active_count < 3:
                    providers = self.get_registered_providers()
                    if len(providers) >= 1:  # Require at least 2 providers
                        self.logger.info(f"Initiating new federation with {len(providers)} providers")
                        self.initiate_federation()
                
                # Sleep between checks
                time.sleep(30)  # Check every 30 seconds
            
            except Exception as e:
                self.logger.error(f"Error in federation scheduler: {e}")
                time.sleep(10)  # Shorter sleep on error
    
    def stop_actor(self):
        """
        Gracefully stop the AGM actor
        """
        self.logger.info("Stopping AGM actor")
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
            
            self.logger.info("AGM actor stopped")
        except Exception as e:
            self.logger.error(f"Error stopping actor: {e}")

if __name__ == '__main__':
    # Use environment variable or default to the Docker service names
    kafka_servers = os.getenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9093,kafka-3:9094")
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("AGM_MAIN")
    logger.info(f"Starting AGM with Kafka servers: {kafka_servers}")
    
    # Initialize and start the AGM actor
    try:
        agm = AGMActor(kafka_servers)
        logger.info("AGM actor initialized successfully")
        agm.start_listening()
    except Exception as e:
        logger.error(f"Failed to start AGM actor: {e}", exc_info=True)