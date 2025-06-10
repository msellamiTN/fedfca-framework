import json
import time
import logging
import os
import base64
import redis
from typing import Dict, List, Optional
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import threading
import yaml

# Import core components (assuming these exist)
from fedfca_core import FedFCA_core as core
from commun.CryptoManager import CryptoManager
from commun.RedisKMS import RedisKMS


class SimplifiedAGMActor:
    """
    Simplified AGM Actor for FedFCA - focuses on core federation functionality
    """
    
    def __init__(self, kafka_servers: str = None):
        # Basic setup
        self.actor_id = f"AGM_{int(time.time())}"
        self.setup_logging()
        
        # Load configuration
        self.config = self.load_config()
        
        # Initialize components
        self.setup_redis()
        self.setup_crypto()
        self.setup_kafka(kafka_servers)
        
        # Core state - simplified
        self.federations = {}  # {fed_id: federation_info}
        self.provider_keys = {}  # {provider_id: key_id}
        self.received_lattices = {}  # {fed_id: {provider_id: lattice}}
        
        # Initialize aggregator
        threshold = self.config.get("server", {}).get("threshold", 0.5)
        self.aggregator = core.Aggregator(threshold=threshold)
        
        self.running = False
        self.logger.info(f"AGM Actor {self.actor_id} initialized")

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.actor_id)

    def load_config(self) -> dict:
        """Load configuration from file or use defaults"""
        config_path = "/data/config.yml"
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f)
        except Exception as e:
            self.logger.warning(f"Config load failed: {e}")
        return {}

    def setup_redis(self):
        """Initialize Redis connection"""
        try:
            host = os.getenv("REDIS_HOST", "datastore")
            port = int(os.getenv("REDIS_PORT", "6379"))
            
            self.redis_client = redis.Redis(
                host=host, port=port, decode_responses=True
            )
            self.redis_client.ping()
            self.logger.info(f"Connected to Redis at {host}:{port}")
        except Exception as e:
            self.logger.error(f"Redis connection failed: {e}")
            self.redis_client = None

    def setup_crypto(self):
        """Initialize crypto manager"""
        try:
            redis_kms = RedisKMS(
                host=os.getenv("REDIS_HOST", "datastore"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                key_prefix="fedfca:kms:agm:"
            )
            self.crypto_manager = CryptoManager(kms=redis_kms)
            self.logger.info("Crypto manager initialized")
        except Exception as e:
            self.logger.error(f"Crypto setup failed: {e}")
            raise

    def setup_kafka(self, kafka_servers: str):
        """Initialize Kafka producer and consumer"""
        self.kafka_servers = kafka_servers or os.getenv(
            "KAFKA_BROKERS", "kafka-1:9092"
        )
        
        # Use exactly 3 topics as requested
        self.topics = [
            'key_exchange',      # Key distribution and acknowledgments
            'lattice_exchange',  # Lattice submissions and training control
            'metrics_exchange'   # Results, metrics and status updates
        ]
        
        try:
            self.create_topics()
            
            # Producer setup
            self.producer = Producer({
                'bootstrap.servers': self.kafka_servers,
                'acks': 'all'
            })
            
            # Consumer setup
            self.consumer = Consumer({
                'bootstrap.servers': self.kafka_servers,
                'group.id': f"agm_group_{self.actor_id}",
                'auto.offset.reset': 'latest'
            })
            
            self.consumer.subscribe(self.topics)
            self.logger.info(f"Kafka initialized with topics: {self.topics}")
            
        except Exception as e:
            self.logger.error(f"Kafka setup failed: {e}")
            raise

    def create_topics(self):
        """Create required Kafka topics"""
        try:
            admin = AdminClient({'bootstrap.servers': self.kafka_servers})
            
            # Check existing topics
            existing = admin.list_topics(timeout=10).topics
            
            # Create missing topics
            new_topics = []
            for topic in self.topics:
                if topic not in existing:
                    new_topics.append(NewTopic(
                        topic=topic,
                        num_partitions=1,
                        replication_factor=1
                    ))
            
            if new_topics:
                futures = admin.create_topics(new_topics)
                for topic, future in futures.items():
                    try:
                        future.result()
                        self.logger.info(f"Created topic: {topic}")
                    except Exception as e:
                        self.logger.error(f"Failed to create {topic}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Topic creation failed: {e}")

    def get_providers(self) -> Dict:
        """Get registered providers from Redis"""
        try:
            if not self.redis_client:
                return {}
            
            providers = self.redis_client.hgetall("federated_providers")
            if providers:
                return {k: json.loads(v) for k, v in providers.items()}
            return {}
        except Exception as e:
            self.logger.error(f"Error getting providers: {e}")
            return {}

    def start_federation(self) -> Optional[str]:
        """Initiate a new federation round"""
        try:
            providers = self.get_providers()
            if len(providers) < 1:
                self.logger.warning(f"Need at least 2 providers, got {len(providers)}")
                return None
            
            # Create federation
            fed_id = f"fed_{int(time.time())}"
            self.federations[fed_id] = {
                'status': 'initializing',
                'providers': list(providers.keys()),
                'start_time': time.time(),
                'expected_lattices': len(providers)
            }
            
            # Initialize lattice storage
            self.received_lattices[fed_id] = {}
            
            # Distribute keys to providers
            self.distribute_keys(fed_id, list(providers.keys()))
            
            # Update status and notify providers
            self.federations[fed_id]['status'] = 'training'
            self.send_training_start(fed_id)
            
            self.logger.info(f"Started federation {fed_id} with {len(providers)} providers")
            return fed_id
            
        except Exception as e:
            self.logger.error(f"Federation start failed: {e}")
            return None

    def distribute_keys(self, fed_id: str, provider_ids: List[str]):
        """Generate and distribute encryption keys"""
        try:
            for provider_id in provider_ids:
                # Generate unique key
                key_id = f"key_{fed_id}_{provider_id}"
                key_metadata = self.crypto_manager.generate_key(
                    key_id=key_id,
                    key_type='fernet',
                    expiration=3600
                )
                
                if key_metadata:
                    self.provider_keys[provider_id] = key_id
                    
                    # Send key to provider
                    message = {
                        'type': 'key_distribution',
                        'federation_id': fed_id,
                        'to_provider': provider_id,
                        'key_id': key_id,
                        'threshold': self.config.get("provider", {}).get("threshold", 0.5),
                        'timestamp': time.time()
                    }
                    
                    self.send_message('key_exchange', message, provider_id)
                    
        except Exception as e:
            self.logger.error(f"Key distribution failed: {e}")

    def send_training_start(self, fed_id: str):
        """Signal providers to start training"""
        message = {
            'type': 'start_training',
            'federation_id': fed_id,
            'timestamp': time.time()
        }
        self.send_message('lattice_exchange', message)

    def send_message(self, topic: str, message: dict, key: str = None):
        """Send message to Kafka topic"""
        try:
            self.producer.produce(
                topic=topic,
                key=key.encode() if key else None,
                value=json.dumps(message).encode(),
                callback=lambda err, msg: self.logger.error(f"Send failed: {err}") if err else None
            )
            self.producer.flush()
            return True
        except Exception as e:
            self.logger.error(f"Message send failed: {e}")
            return False

    def process_lattice_submission(self, message: dict):
        """Process incoming lattice from provider"""
        try:
            fed_id = message.get('federation_id')
            provider_id = message.get('from_provider')
            encrypted_lattice = message.get('encrypted_lattice')
            
            if not all([fed_id, provider_id, encrypted_lattice]):
                self.logger.error("Invalid lattice submission")
                return False
            
            # Decrypt lattice
            key_id = self.provider_keys.get(provider_id)
            if not key_id:
                self.logger.error(f"No key for provider {provider_id}")
                return False
            
            # Decode and decrypt
            encrypted_data = base64.b64decode(encrypted_lattice)
            decrypted_data = self.crypto_manager.decrypt_with_key_id(encrypted_data, key_id)
            
            if not decrypted_data:
                self.logger.error(f"Decryption failed for {provider_id}")
                return False
            
            # Parse lattice data
            lattice_data = json.loads(decrypted_data)
            
            # Store lattice
            if fed_id not in self.received_lattices:
                self.received_lattices[fed_id] = {}
            
            self.received_lattices[fed_id][provider_id] = lattice_data
            self.logger.info(f"Received lattice from {provider_id}")
            
            # Check if we have all lattices
            federation = self.federations.get(fed_id, {})
            received_count = len(self.received_lattices[fed_id])
            expected_count = federation.get('expected_lattices', 0)
            
            if received_count >= expected_count:
                self.aggregate_federation(fed_id)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Lattice processing failed: {e}")
            return False

    def aggregate_federation(self, fed_id: str):
        """Aggregate all lattices for a federation"""
        try:
            self.logger.info(f"Starting aggregation for {fed_id}")
            
            # Get all lattices
            lattices = list(self.received_lattices[fed_id].values())
            
            # Perform aggregation
            start_time = time.time()
            self.aggregator.aggregate(lattices)
            aggregation_time = time.time() - start_time
            
            # Get results
            global_lattice = self.aggregator.global_lattice
            global_stability = self.aggregator.global_stability
            
            # Update federation status
            federation = self.federations[fed_id]
            federation['status'] = 'completed'
            federation['completion_time'] = time.time()
            federation['global_stability'] = global_stability
            
            # Calculate metrics
            total_time = federation['completion_time'] - federation['start_time']
            
            # Send results
            result_message = {
                'type': 'federation_completed',
                'federation_id': fed_id,
                'global_stability': global_stability,
                'aggregation_time': aggregation_time,
                'total_time': total_time,
                'participants': len(lattices),
                'timestamp': time.time()
            }
            
            self.send_message('metrics_exchange', result_message)
            
            # Store results
            self.save_results(fed_id, global_lattice, result_message)
            
            # Cleanup
            del self.received_lattices[fed_id]
            
            self.logger.info(f"Federation {fed_id} completed. Stability: {global_stability:.4f}")
            
        except Exception as e:
            self.logger.error(f"Aggregation failed: {e}")

    def save_results(self, fed_id: str, lattice, metrics: dict):
        """Save federation results to Redis"""
        try:
            if not self.redis_client:
                return
            
            result_data = {
                'lattice': lattice,
                'metrics': metrics,
                'timestamp': time.time()
            }
            
            key = f"federation_result:{fed_id}"
            self.redis_client.set(key, json.dumps(result_data))
            self.redis_client.expire(key, 86400)  # 24 hours
            
        except Exception as e:
            self.logger.error(f"Save results failed: {e}")

    def process_message(self, kafka_message):
        """Process incoming Kafka message"""
        try:
            topic = kafka_message.topic()
            message = json.loads(kafka_message.value().decode())
            msg_type = message.get('type', '')
            
            self.logger.debug(f"Processing {msg_type} from topic {topic}")
            
            # Route based on topic
            if topic == 'key_exchange':
                self.handle_key_message(message)
            elif topic == 'lattice_exchange':
                self.handle_lattice_message(message)
            elif topic == 'metrics_exchange':
                self.handle_metrics_message(message)
            else:
                self.logger.warning(f"Unknown topic: {topic}")
                
        except Exception as e:
            self.logger.error(f"Message processing failed: {e}")

    def handle_key_message(self, message):
        """Handle key exchange messages"""
        msg_type = message.get('type')
        
        if msg_type == 'key_acknowledged':
            provider_id = message.get('from_provider')
            fed_id = message.get('federation_id')
            status = message.get('status')
            
            self.logger.info(f"Key acknowledged by {provider_id}: {status}")
            
            # Could add logic here to track key acknowledgments if needed

    def handle_lattice_message(self, message):
        """Handle lattice exchange messages"""
        msg_type = message.get('type')
        
        if msg_type == 'lattice_submission':
            self.process_lattice_submission(message)
        elif msg_type == 'training_status':
            provider_id = message.get('from_provider')
            status = message.get('status')
            self.logger.info(f"Training status from {provider_id}: {status}")

    def handle_metrics_message(self, message):
        """Handle metrics exchange messages"""
        msg_type = message.get('type')
        
        if msg_type == 'provider_metrics':
            provider_id = message.get('from_provider')
            fed_id = message.get('federation_id')
            metrics = message.get('metrics', {})
            
            # Store provider metrics if needed
            self.logger.debug(f"Received metrics from {provider_id}")

    def start_listening(self):
        """Start the main message processing loop"""
        self.logger.info("Starting AGM message listener")
        self.running = True
        
        # Start federation scheduler
        scheduler_thread = threading.Thread(target=self.federation_scheduler)
        scheduler_thread.daemon = True
        scheduler_thread.start()
        
        try:
            while self.running:
                try:
                    message = self.consumer.poll(1.0)
                    if message is None:
                        continue
                        
                    if message.error():
                        if message.error().code() != KafkaError._PARTITION_EOF:
                            self.logger.error(f"Consumer error: {message.error()}")
                        continue
                    
                    self.process_message(message)
                    
                except Exception as e:
                    self.logger.error(f"Error in message loop: {e}")
                    time.sleep(1)
                    
        except KeyboardInterrupt:
            self.logger.info("Shutdown requested")
        finally:
            self.stop()

    def federation_scheduler(self):
        """Periodically start new federations"""
        while self.running:
            try:
                # Count active federations
                active_count = sum(1 for fed in self.federations.values() 
                                 if fed['status'] in ['initializing', 'training'])
                
                # Start new federation if capacity available
                if active_count < 2:  # Limit concurrent federations
                    providers = self.get_providers()
                    if len(providers) >= 2:
                        self.start_federation()
                
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                time.sleep(10)

    def stop(self):
        """Stop the AGM actor gracefully"""
        self.logger.info("Stopping AGM actor")
        self.running = False
        
        try:
            self.consumer.close()
            self.logger.info("AGM actor stopped")
        except Exception as e:
            self.logger.error(f"Stop error: {e}")


if __name__ == '__main__':
    kafka_servers = os.getenv("KAFKA_BROKERS", "kafka-1:9092")
    
    try:
        agm = SimplifiedAGMActor(kafka_servers)
        agm.start_listening()
    except Exception as e:
        logging.error(f"Failed to start AGM: {e}")