import json
import time
import logging
import uuid as uuid
import os
from datetime import datetime
import base64
from cryptography.fernet import Fernet
import redis
from confluent_kafka import Producer, Consumer, KafkaError
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import yaml
# Add these imports at the top of almactor_light_clean.py
from typing import Dict, Any, Optional, List, Tuple, Union
from commun.RedisKMS import RedisKMS
# Import core components
from fedfca_core import FedFCA_core as core
from commun.logactor import LoggerActor
from commun.FedFcaMonitor import FedFcaMonitor
from commun.CryptoManager import CryptoManager
from commun.RedisKMS import RedisKMS
class ALMActor:
    """
    Simplified Actor for Local Model (ALM) in FedFCA.
    Uses only 3 topics: key_exchange, lattice_exchange, metrics_exchange
    """
    
    def __init__(self, kafka_servers=None):
        # Initialize actor ID and logging
        self.actor_id = f"ALM_{os.environ.get('ACTOR_ID_SUFFIX', uuid.uuid4().hex[:6])}"
        logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
        self.logger = logging.getLogger(self.actor_id)
        
        # Initialize state
        self.federation_id = None
        self.provider_id = os.environ.get("PROVIDER_ID", f"provider_{uuid.uuid4().hex[:8]}")
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
        
        # Initialize RedisKMS for key management - use 'datastore' as the default host
        redis_host = os.getenv("REDIS_HOST", "datastore")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        redis_password = os.getenv("REDIS_PASSWORD", "mysecred")
        redis_user = os.getenv("REDIS_USER", "fedfac")
        
        self.logger.info(f"Initializing RedisKMS with host={redis_host}, port={redis_port}")
        
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
        self.crypto_manager = CryptoManager(kms=redis_kms)
        self.lattice_builder = core.FasterFCA(
            threshold=self.config.get("provider", {}).get("threshold", 0.5),
            context_sensitivity=self.config.get("provider", {}).get("context_sensitivity", 0.2)
        )
        # Initialize local context
        self.local_context = self._load_local_context()
        if self.local_context:
            self.lattice_builder.extract_formal_context(
                self._format_context_for_fca(self.local_context)
            )
            self.lattice_builder.read_context(self.lattice_builder.formal_context)
        
        self.logger.info(f"ALM Actor {self.actor_id} initialized")
    
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
                key_prefix="fedfca:kms:alm:",
    
            )
            self.crypto_manager = CryptoManager(kms=redis_kms)
            self.logger.info("Redis and KMS initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis/KMS: {e}")
            raise
    
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
                'group.id': f'alm_group_{self.provider_id}',
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
        return {}
    
    def _send_message(self, topic, message_type, payload):
        """Send a message to a Kafka topic"""
        try:
            message = {
                'message_id': str(uuid.uuid4()),
                'timestamp': int(time.time() * 1000),
                'sender_id': self.provider_id,
                'message_type': message_type,
                'payload': payload
            }
            
            if self.federation_id:
                message['federation_id'] = self.federation_id
            
            self.producer.produce(
                topic=topic,
                key=self.provider_id,
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
            
            if msg.topic() == self.topics['key_exchange']:
                self._handle_key_exchange(message, message_type)
                
            elif msg.topic() == self.topics['lattice_exchange']:
                self._handle_lattice_exchange(message, message_type)
                
            elif msg.topic() == self.topics['metrics_exchange']:
                self._handle_metrics_exchange(message, message_type)
                
        except Exception as e:
            self.logger.error(f"Error processing message: {e}", exc_info=True)
    
    def _handle_key_exchange(self, message, message_type):
        """Handle key exchange messages"""
        if message_type == 'key_distribution':
            key_id = message['payload'].get('key_id')
            encrypted_key = message['payload'].get('encrypted_key')
            
            if key_id and encrypted_key:
                try:
                    # Store the key using KMS
                    self.crypto_manager.kms.store_key(key_id, encrypted_key)
                    self.encryption_key = key_id
                    self.federation_id = message.get('federation_id')
                    self.logger.info(f"Received encryption key {key_id}")
                    
                    # Acknowledge key receipt
                    self._send_message(
                        topic=self.topics['key_exchange'],
                        message_type='key_ack',
                        payload={'key_id': key_id, 'status': 'received'}
                    )
                except Exception as e:
                    self.logger.error(f"Failed to process key: {e}")
    
    def _build_lattice(self) -> Dict[str, Any]:
        """Build lattice using FasterFCA"""
        try:
            # Ensure we have a valid context
            if not hasattr(self.lattice_builder, 'formal_context') or not self.lattice_builder.formal_context:
                self.logger.warning("No formal context available, building from local context")
                self.lattice_builder.extract_formal_context(
                    self._format_context_for_fca(self.local_context)
                )
                self.lattice_builder.read_context(self.lattice_builder.formal_context)
            
            # Generate the lattice
            self.lattice_builder.get_bipartite_cliques()
            lattice = self.lattice_builder.condense_list(self.lattice_builder.dictBC)
            
            # Format the result
            return {
                'concepts': lattice,
                'objects': self.local_context['objects'],
                'attributes': self.local_context['attributes'],
                'provider_id': self.provider_id,
                'timestamp': int(time.time() * 1000)
            }
            
        except Exception as e:
            self.logger.error(f"Error building lattice: {e}", exc_info=True)
            raise

    def _handle_lattice_exchange(self, message, message_type):
        """Handle lattice exchange messages"""
        if message_type == 'lattice_request':
            try:
                # Generate local lattice
                lattice = self._build_lattice()
                
                # Encrypt the lattice if we have a key
                if self.encryption_key:
                    lattice_data = json.dumps(lattice).encode('utf-8')
                    encrypted_data = self.crypto_manager.encrypt(
                        self.encryption_key,
                        lattice_data
                    )
                    # Convert to base64 for safe JSON serialization
                    lattice = {
                        'encrypted': True,
                        'data': base64.b64encode(encrypted_data).decode('utf-8'),
                        'key_id': self.encryption_key,
                        'provider_id': self.provider_id,
                        'timestamp': int(time.time() * 1000)
                    }
                
                # Send the lattice to AGM
                self._send_message(
                    topic=self.topics['lattice_exchange'],
                    message_type='lattice_update',
                    payload=lattice
                )
                self.logger.info("Sent lattice update to AGM")
                
            except Exception as e:
                self.logger.error(f"Failed to process lattice request: {e}", exc_info=True)
    
    def _handle_metrics_exchange(self, message, message_type):
        """Handle metrics exchange messages"""
        if message_type == 'health_check':
            self._send_message(
                topic=self.topics['metrics_exchange'],
                message_type='health_status',
                payload={
                    'provider_id': self.provider_id,
                    'status': 'healthy',
                    'timestamp': int(time.time() * 1000)
                }
            )
    
    def _format_context_for_fca(self, context_data: Dict[str, Any]) -> List[str]:
        """Format context data for FasterFCA"""
        formatted_lines = []
        for obj_idx, obj_name in enumerate(context_data['objects'], 1):
            attrs = [
                attr_idx + 1 
                for attr_idx, has_attr in enumerate(context_data['incidence'][obj_idx - 1])
                if has_attr
            ]
            formatted_lines.append(f"{obj_idx}: {set(attrs)}")
        return formatted_lines

    def _load_local_context(self) -> Dict[str, Any]:
        """Load local context for lattice generation"""
        try:
            # Try to load context from Redis if available
            if hasattr(self, 'redis_client'):
                context_data = self.redis_client.get(f"{self.provider_id}:context")
                if context_data:
                    return json.loads(context_data)
            
            # Fallback to config or default context
            context = self.config.get('context', {
                'objects': [f'obj_{i}' for i in range(1, 3)],
                'attributes': [f'attr_{i}' for i in range(1, 3)],
                'incidence': [
                    [True, False],
                    [False, True]
                ]
            })
            
            # Store the context in Redis for future use
            if hasattr(self, 'redis_client'):
                self.redis_client.set(
                    f"{self.provider_id}:context",
                    json.dumps(context),
                    ex=86400  # 24h expiry
                )
            
            return context
            
        except Exception as e:
            self.logger.error(f"Error loading local context: {e}")
            # Return a minimal valid context
            return {
                'objects': ['obj1', 'obj2'],
                'attributes': ['attr1', 'attr2'],
                'incidence': [
                    [True, False],
                    [False, True]
                ]
            }
    
    def run(self):
        """Main processing loop"""
        self.logger.info("Starting ALM Actor main loop")
        
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
            self.logger.info("ALM Actor stopped")
    
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
        self.logger.info("ALM Actor resources cleaned up")


def main():
    """Main entry point for the ALM Actor"""
    try:
        with ALMActor() as actor:
            actor.run()
    except Exception as e:
        logging.error(f"ALM Actor failed: {e}", exc_info=True)
        return 1
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
