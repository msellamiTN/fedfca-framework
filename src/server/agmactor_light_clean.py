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
# Add these imports at the top of almactor_light_clean.py
from typing import Dict, Any, Optional, List, Tuple, Union
from commun.RedisKMS import RedisKMS
# Import core components
from fedfca_core import FedFCA_core as core
from commun.logactor import LoggerActor
from commun.FedFcaMonitor import FedFcaMonitor
from commun.CryptoManager import CryptoManager
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
# Type aliases
LatticeData = Dict[str, Any]
ProviderId = str
FederationId = str

class AGMActor:
    """
    Simplified Actor for Global Model (AGM) in FedFCA.
    Uses only 3 topics: key_exchange, lattice_exchange, metrics_exchange
    """
    
    def __init__(self, kafka_servers=None):
        # Initialize actor ID and logging
        self.actor_id = f"AGM_{os.environ.get('ACTOR_ID_SUFFIX', uuid.uuid4().hex[:6])}"
        logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
        self.logger = logging.getLogger(self.actor_id)
        
        # Initialize state
        self.federation_id = os.environ.get("FEDERATION_ID", f"federation_{uuid.uuid4().hex[:8]}")
        self.providers = set()
        self.encryption_key = None
        self.running = True
        
        # Load configuration
        self.config = self._load_config()
        
        # Initialize Redis and KMS
        self._init_redis()
        
        # Initialize Kafka
        self.kafka_servers = kafka_servers or os.environ.get("KAFKA_BROKERS", "kafka-1:9092")
        self.topics = {
            'key_exchange': 'key_exchange',
            'lattice_exchange': 'lattice_exchange',
            'metrics_exchange': 'metrics_exchange'
        }
        
        # Initialize Kafka producer and consumer
        self._init_kafka()
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
     
        # Track received lattices for aggregation
        self.received_lattices: Dict[FederationId, Dict[ProviderId, LatticeData]] = {}
        self.aggregation_threshold = int(self.config.get("min_providers", 1))
        
        self.logger.info(f"AGM Actor {self.actor_id} initialized for federation {self.federation_id}")
    
    def _init_redis(self):
        """Initialize Redis connection and KMS"""
        redis_host = os.getenv("REDIS_HOST", "datastore")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        redis_password = os.getenv("REDIS_PASSWORD", "mysecred")
        redis_user = os.getenv("REDIS_USER", "fedfac")
        
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
 
                decode_responses=True
            )
            self.redis_client.ping()
            
            # Initialize KMS
            redis_kms = RedisKMS(
                host=redis_host,
                port=redis_port,
                db=0,
                key_prefix="fedfca:kms:agm:",
 
            )
            self.crypto_manager = CryptoManager(kms=redis_kms)
            
            # Generate or load federation key
            self._setup_federation_key()
            
            self.logger.info("Redis and KMS initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis/KMS: {e}")
            raise
    
    def _setup_federation_key(self):
        """Generate or load federation encryption key"""
        key_id = f"fedkey_{self.federation_id}"
        
        # Try to load existing key
        if self.crypto_manager.kms.key_exists(key_id):
            self.encryption_key = key_id
            self.logger.info(f"Loaded existing federation key: {key_id}")
            return
        
        # Generate new key if not exists
        try:
            key_data = self.crypto_manager.generate_key()
            self.crypto_manager.kms.store_key(key_id, key_data)
            self.encryption_key = key_id
            self.logger.info(f"Generated new federation key: {key_id}")
            
            # Distribute key to all providers
            self._distribute_encryption_key()
            
        except Exception as e:
            self.logger.error(f"Failed to generate federation key: {e}")
            raise
    
    def _distribute_encryption_key(self):
        """Distribute encryption key to all providers"""
        if not self.encryption_key:
            self.logger.warning("No encryption key available to distribute")
            return
        
        try:
            key_data = self.crypto_manager.kms.get_key(self.encryption_key)
            
            # In a real implementation, you would encrypt this with each provider's public key
            # For simplicity, we'll just send it directly in this example
            
            self._send_message(
                topic=self.topics['key_exchange'],
                message_type='key_distribution',
                payload={
                    'key_id': self.encryption_key,
                    'key_data': key_data,
                    'federation_id': self.federation_id
                }
            )
            
            self.logger.info(f"Distributed encryption key {self.encryption_key} to federation")
            
        except Exception as e:
            self.logger.error(f"Failed to distribute encryption key: {e}")
    
    def _init_kafka(self):
        """Initialize Kafka producer and consumer"""
        try:
            # Producer configuration
            producer_config = {
                'bootstrap.servers': self.kafka_servers,
                'message.max.bytes': 10485760,  # 10MB
                'compression.type': 'gzip',
                'retries': 3,
                'acks': 'all'
            }
            self.producer = Producer(producer_config)
            
            # Consumer configuration
            consumer_config = {
                'bootstrap.servers': self.kafka_servers,
                'group.id': f'agm_group_{self.federation_id}',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
                'session.timeout.ms': 30000,
                'max.poll.interval.ms': 300000
            }
            self.consumer = Consumer(consumer_config)
            
            # Subscribe to all 3 topics
            self.consumer.subscribe(list(self.topics.values()))
            self.logger.info(f"Subscribed to topics: {list(self.topics.values())}")
            
            # Verify connectivity
            self.producer.list_topics(timeout=10.0)
            self.logger.info("Successfully connected to Kafka")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka: {e}")
            raise
    
    def _load_config(self):
        """Load configuration from file or environment"""
        config_path = os.environ.get('CONFIG_PATH', '/data/config.yml')
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f) or {}
        except Exception as e:
            self.logger.warning(f"Failed to load config from {config_path}: {e}")
        
        # Default configuration
        return {
            'threshold': 0.5,
            'context_sensitivity': 0.2,
            'min_providers': 1
        }
    
    def _send_message(self, topic, message_type, payload):
        """Send a message to a Kafka topic"""
        try:
            message = {
                'message_id': str(uuid.uuid4()),
                'timestamp': int(time.time() * 1000),
                'sender_id': self.actor_id,
                'federation_id': self.federation_id,
                'message_type': message_type,
                'payload': payload
            }
            
            self.producer.produce(
                topic=topic,
                key=self.actor_id,
                value=json.dumps(message).encode('utf-8'),
                callback=lambda err, msg, mt=message_type: self._delivery_report(err, msg, mt)
            )
            self.producer.flush(timeout=5.0)
            
        except Exception as e:
            self.logger.error(f"Failed to send {message_type} message: {e}")
    
    def _delivery_report(self, err, msg, message_type):
        """Callback for message delivery reports"""
        if err is not None:
            self.logger.error(f"Message delivery failed for {message_type}: {err}")
    
    def process_message(self, msg):
        """Process incoming Kafka messages"""
        try:
            message = json.loads(msg.value().decode('utf-8'))
            message_type = message.get('message_type')
            sender_id = message.get('sender_id')
            
            if msg.topic() == self.topics['key_exchange']:
                self._handle_key_exchange(message, message_type, sender_id)
                
            elif msg.topic() == self.topics['lattice_exchange']:
                self._handle_lattice_exchange(message, message_type, sender_id)
                
            elif msg.topic() == self.topics['metrics_exchange']:
                self._handle_metrics_exchange(message, message_type, sender_id)
                
        except Exception as e:
            self.logger.error(f"Error processing message: {e}", exc_info=True)
    
    def _handle_key_exchange(self, message, message_type, sender_id):
        """Handle key exchange messages"""
        try:
            payload = message.get('payload', {})
            
            if message_type == 'key_ack':
                key_id = payload.get('key_id')
                status = payload.get('status')
                
                if status == 'received':
                    self.providers.add(sender_id)
                    self.logger.info(f"Provider {sender_id} acknowledged key {key_id}")
                    
                    # Send a confirmation back to the provider
                    self._send_message(
                        topic=self.topics['key_exchange'],
                        message_type='key_ack_received',
                        payload={
                            'key_id': key_id,
                            'provider_id': sender_id,
                            'federation_id': self.federation_id,
                            'status': 'confirmed'
                        }
                    )
            
            elif message_type == 'key_request':
                # A provider is requesting the federation key
                provider_id = payload.get('provider_id')
                if provider_id and self.encryption_key:
                    self._distribute_encryption_key(provider_id)
                    
        except Exception as e:
            self.logger.error(f"Error processing key exchange: {e}", exc_info=True)
    
    def _handle_lattice_exchange(self, message, message_type, sender_id):
        """Handle lattice exchange messages"""
        if message_type == 'lattice_update':
            try:
                lattice_data = message['payload']
                
                # Verify the sender is a known provider
                if sender_id not in self.providers:
                    self.logger.warning(f"Received lattice from unknown provider: {sender_id}")
                    return
                
                # Initialize storage for this federation if needed
                if self.federation_id not in self.received_lattices:
                    self.received_lattices[self.federation_id] = {}
                
                # Store the lattice data
                self.received_lattices[self.federation_id][sender_id] = lattice_data
                self.logger.info(f"Received lattice update from {sender_id}")
                
                # Check if we have enough lattices to aggregate
                if len(self.received_lattices[self.federation_id]) >= self.aggregation_threshold:
                    self._aggregate_lattices()
                else:
                    remaining = self.aggregation_threshold - len(self.received_lattices[self.federation_id])
                    self.logger.info(f"Waiting for {remaining} more lattice(s) before aggregation")
                    
            except Exception as e:
                self.logger.error(f"Failed to process lattice update: {e}", exc_info=True)
    
    def _aggregate_lattices(self):
        """Aggregate received lattices using FasterFCA"""
        fed_id = self.federation_id
        if fed_id not in self.received_lattices or not self.received_lattices[fed_id]:
            self.logger.warning("No lattices to aggregate")
            return
        
        try:
            lattices = list(self.received_lattices[fed_id].values())
            num_lattices = len(lattices)
            
            if num_lattices < self.aggregation_threshold:
                self.logger.info(f"Not enough lattices for aggregation. Have {num_lattices}, need {self.aggregation_threshold}")
                return
            
            self.logger.info(f"Aggregating {num_lattices} lattices for federation {fed_id}")
            
            # Combine all concepts from all lattices
            all_concepts = []
            provider_ids = set()
            
            for lattice in lattices:
                if 'encrypted' in lattice and lattice['encrypted']:
                    try:
                        # Decrypt the lattice data
                        encrypted_data = base64.b64decode(lattice['data'])
                        decrypted_data = self.crypto_manager.decrypt(
                            lattice['key_id'],
                            encrypted_data
                        )
                        lattice = json.loads(decrypted_data.decode('utf-8'))
                    except Exception as e:
                        self.logger.error(f"Failed to decrypt lattice: {e}")
                        continue
                
                if 'concepts' in lattice:
                    all_concepts.extend(lattice['concepts'])
                if 'provider_id' in lattice:
                    provider_ids.add(lattice['provider_id'])
            
            # Perform the actual aggregation
            if all_concepts:
                # This is a simplified example - in practice, you'd use the lattice_aggregator
                # to combine the concepts in a more sophisticated way
                aggregated_concepts = self._combine_concepts(all_concepts)
                
                # Store the aggregated result
                result = {
                    'federation_id': fed_id,
                    'concepts': aggregated_concepts,
                    'providers': list(provider_ids),
                    'timestamp': int(time.time() * 1000),
                    'num_concepts': len(aggregated_concepts)
                }
                
                # Store in Redis
                self._store_aggregation_result(fed_id, result)
                
                # Notify providers of successful aggregation
                self._send_message(
                    topic=self.topics['metrics_exchange'],
                    message_type='aggregation_complete',
                    payload=result
                )
                
                self.logger.info(f"Successfully aggregated {len(provider_ids)} lattices into {len(aggregated_concepts)} concepts")
            
            # Clear the received lattices after successful aggregation
            self.received_lattices[fed_id] = {}
            
        except Exception as e:
            self.logger.error(f"Failed to aggregate lattices: {e}", exc_info=True)
    
    def _combine_concepts(self, concepts: List[Any]) -> List[Any]:
        """Combine concepts from different providers"""
        # This is a simplified example - in practice, you'd use the lattice_aggregator
        # to combine the concepts in a more sophisticated way
        
        # For now, just return the union of all concepts (with deduplication)
        seen = set()
        unique_concepts = []
        
        for concept in concepts:
            # Create a hashable representation of the concept
            concept_repr = json.dumps(concept, sort_keys=True)
            if concept_repr not in seen:
                seen.add(concept_repr)
                unique_concepts.append(concept)
        
        return unique_concepts
    
    def _store_aggregation_result(self, fed_id: str, result: Dict[str, Any]) -> bool:
        """Store aggregation result in Redis"""
        try:
            if not hasattr(self, 'redis_client'):
                return False
                
            key = f"fedfca:aggregation:{fed_id}:{int(time.time())}"
            self.redis_client.set(
                key,
                json.dumps(result),
                ex=86400  # 24h expiry
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to store aggregation result: {e}")
            return False
    
    def _handle_metrics_exchange(self, message, message_type, sender_id):
        """Handle metrics exchange messages"""
        if message_type == 'health_check':
            self._send_message(
                topic=self.topics['metrics_exchange'],
                message_type='health_status',
                payload={
                    'status': 'healthy',
                    'federation_id': self.federation_id,
                    'providers': list(self.providers),
                    'timestamp': int(time.time() * 1000)
                }
            )
    
    def run(self):
        """Main processing loop"""
        self.logger.info("Starting AGM Actor main loop")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self.logger.error(f"Kafka error: {msg.error()}")
                    continue
                    
                self.process_message(msg)
                
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            self.consumer.close()
            self.producer.flush()
            self.logger.info("AGM Actor stopped")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - clean up resources"""
        self.running = False
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'producer'):
            self.producer.flush()
        self.logger.info("AGM Actor resources cleaned up")


def main():
    """Main entry point for the AGM Actor"""
    try:
        with AGMActor() as actor:
            actor.run()
    except Exception as e:
        logging.error(f"AGM Actor failed: {e}", exc_info=True)
        return 1
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
