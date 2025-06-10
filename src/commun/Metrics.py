import json
import time
import psutil
import threading
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any, Optional
import redis
import kafka
from kafka import KafkaConsumer, KafkaProducer
import logging

class FedFCAMetricsCollector:
    """
    Comprehensive metrics collector for FedFCA experiments to address
    computational vs communication overhead analysis concerns.
    """
    
    def __init__(self, actor_id: str, redis_host: str = "datastore", 
                 kafka_brokers: List[str] = ["kafka-1:9092"]):
        self.actor_id = actor_id
        self.redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)
        self.kafka_brokers = kafka_brokers
        self.metrics = defaultdict(list)
        self.start_time = None
        self.monitoring = False
        self.monitor_thread = None
        
        # Initialize logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"MetricsCollector-{actor_id}")
        
    def start_experiment_timing(self, experiment_name: str):
        """Start timing for a specific experiment"""
        self.start_time = time.time()
        self.experiment_name = experiment_name
        self.metrics = defaultdict(list)
        self.monitoring = True
        
        # Start background monitoring
        self.monitor_thread = threading.Thread(target=self._monitor_resources)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        self.logger.info(f"Started experiment: {experiment_name}")
        
    def stop_experiment_timing(self):
        """Stop timing and monitoring"""
        if self.start_time:
            total_time = time.time() - self.start_time
            self.metrics['total_experiment_time'].append(total_time)
            
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
            
        self.logger.info("Stopped experiment timing")
        
    def _monitor_resources(self):
        """Background monitoring of system resources"""
        while self.monitoring:
            try:
                # CPU and Memory metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                
                self.metrics['cpu_usage'].append({
                    'timestamp': time.time(),
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory.percent,
                    'memory_used_mb': memory.used / (1024 * 1024)
                })
                
                # Network I/O metrics
                net_io = psutil.net_io_counters()
                self.metrics['network_io'].append({
                    'timestamp': time.time(),
                    'bytes_sent': net_io.bytes_sent,
                    'bytes_recv': net_io.bytes_recv,
                    'packets_sent': net_io.packets_sent,
                    'packets_recv': net_io.packets_recv
                })
                
                time.sleep(1)  # Monitor every second
                
            except Exception as e:
                self.logger.error(f"Error monitoring resources: {e}")
                
    def record_computation_time(self, operation: str, start_time: float, end_time: float):
        """Record computation time for specific operations"""
        computation_time = end_time - start_time
        self.metrics['computation_times'].append({
            'operation': operation,
            'duration_seconds': computation_time,
            'timestamp': end_time,
            'actor_id': self.actor_id
        })
        
    def record_communication_overhead(self, operation: str, message_size: int, 
                                    latency: float, overhead_type: str = "kafka"):
        """Record communication overhead metrics"""
        self.metrics['communication_overhead'].append({
            'operation': operation,
            'message_size_bytes': message_size,
            'latency_seconds': latency,
            'overhead_type': overhead_type,  # kafka, redis, key_exchange, lattice_transmission
            'timestamp': time.time(),
            'actor_id': self.actor_id
        })
        
    def record_kafka_metrics(self):
        """Record Kafka-specific metrics"""
        try:
            # Create a consumer to get metadata
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_brokers,
                consumer_timeout_ms=1000
            )
            
            # Get topic metadata
            metadata = consumer.list_consumer_groups()
            topics = consumer.topics()
            
            self.metrics['kafka_metrics'].append({
                'timestamp': time.time(),
                'topics_count': len(topics),
                'consumer_groups': len(metadata),
                'actor_id': self.actor_id
            })
            
            consumer.close()
            
        except Exception as e:
            self.logger.error(f"Error collecting Kafka metrics: {e}")
            
    def record_redis_metrics(self):
        """Record Redis datastore metrics"""
        try:
            info = self.redis_client.info()
            
            self.metrics['redis_metrics'].append({
                'timestamp': time.time(),
                'used_memory': info.get('used_memory', 0),
                'connected_clients': info.get('connected_clients', 0),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'actor_id': self.actor_id
            })
            
        except Exception as e:
            self.logger.error(f"Error collecting Redis metrics: {e}")
            
    def record_lattice_operation(self, operation: str, lattice_size: int, 
                               transmission_time: float, computation_time: float):
        """Record FCA lattice-specific operations"""
        self.metrics['lattice_operations'].append({
            'operation': operation,  # build, merge, transmit, receive
            'lattice_size': lattice_size,
            'transmission_time_seconds': transmission_time,
            'computation_time_seconds': computation_time,
            'total_time_seconds': transmission_time + computation_time,
            'timestamp': time.time(),
            'actor_id': self.actor_id
        })
        
    def record_key_exchange_overhead(self, operation: str, key_size: int, 
                                   exchange_time: float):
        """Record cryptographic key exchange overhead"""
        self.metrics['key_exchange'].append({
            'operation': operation,  # generate, exchange, verify
            'key_size_bits': key_size,
            'exchange_time_seconds': exchange_time,
            'timestamp': time.time(),
            'actor_id': self.actor_id
        })
        
    def record_microservice_overhead(self, service_name: str, request_time: float,
                                   response_size: int):
        """Record microservice communication overhead"""
        self.metrics['microservice_overhead'].append({
            'service_name': service_name,  # agm_actor, alm_actor1, dataset-preparator, etc.
            'request_time_seconds': request_time,
            'response_size_bytes': response_size,
            'timestamp': time.time(),
            'actor_id': self.actor_id
        })
        
    def calculate_overhead_breakdown(self) -> Dict[str, Any]:
        """Calculate detailed breakdown of computational vs communication overhead"""
        breakdown = {
            'computation_overhead': {
                'total_time': 0,
                'operations': defaultdict(float)
            },
            'communication_overhead': {
                'total_time': 0,
                'by_type': defaultdict(float),
                'total_bytes': 0
            },
            'architecture_overhead': {
                'kafka_time': 0,
                'redis_time': 0,
                'microservice_time': 0,
                'key_exchange_time': 0
            }
        }
        
        # Calculate computation overhead
        for comp in self.metrics['computation_times']:
            duration = comp['duration_seconds']
            operation = comp['operation']
            breakdown['computation_overhead']['total_time'] += duration
            breakdown['computation_overhead']['operations'][operation] += duration
            
        # Calculate communication overhead
        for comm in self.metrics['communication_overhead']:
            latency = comm['latency_seconds']
            msg_size = comm['message_size_bytes']
            overhead_type = comm['overhead_type']
            
            breakdown['communication_overhead']['total_time'] += latency
            breakdown['communication_overhead']['by_type'][overhead_type] += latency
            breakdown['communication_overhead']['total_bytes'] += msg_size
            
        # Calculate architecture-specific overhead
        for lattice in self.metrics['lattice_operations']:
            breakdown['architecture_overhead']['kafka_time'] += lattice['transmission_time_seconds']
            
        for key_ex in self.metrics['key_exchange']:
            breakdown['architecture_overhead']['key_exchange_time'] += key_ex['exchange_time_seconds']
            
        for micro in self.metrics['microservice_overhead']:
            breakdown['architecture_overhead']['microservice_time'] += micro['request_time_seconds']
            
        return breakdown
        
    def generate_comparison_metrics(self) -> Dict[str, Any]:
        """Generate metrics for comparison with centralized FCA"""
        overhead_breakdown = self.calculate_overhead_breakdown()
        
        total_time = self.metrics['total_experiment_time'][0] if self.metrics['total_experiment_time'] else 0
        pure_computation = overhead_breakdown['computation_overhead']['total_time']
        communication_overhead = overhead_breakdown['communication_overhead']['total_time']
        architecture_overhead = sum(overhead_breakdown['architecture_overhead'].values())
        
        comparison_metrics = {
            'experiment_name': getattr(self, 'experiment_name', 'unknown'),
            'actor_id': self.actor_id,
            'timestamp': datetime.now().isoformat(),
            'timing_breakdown': {
                'total_experiment_time_seconds': total_time,
                'pure_computation_time_seconds': pure_computation,
                'communication_overhead_seconds': communication_overhead,
                'architecture_overhead_seconds': architecture_overhead,
                'computation_percentage': (pure_computation / total_time * 100) if total_time > 0 else 0,
                'communication_percentage': (communication_overhead / total_time * 100) if total_time > 0 else 0,
                'architecture_percentage': (architecture_overhead / total_time * 100) if total_time > 0 else 0
            },
            'detailed_breakdown': overhead_breakdown,
            'resource_usage': {
                'peak_cpu_percent': max([r['cpu_percent'] for r in self.metrics['cpu_usage']], default=0),
                'peak_memory_mb': max([r['memory_used_mb'] for r in self.metrics['cpu_usage']], default=0),
                'total_network_bytes_sent': self.metrics['network_io'][-1]['bytes_sent'] - self.metrics['network_io'][0]['bytes_sent'] if self.metrics['network_io'] else 0,
                'total_network_bytes_recv': self.metrics['network_io'][-1]['bytes_recv'] - self.metrics['network_io'][0]['bytes_recv'] if self.metrics['network_io'] else 0
            },
            'federated_specific_metrics': {
                'lattice_operations_count': len(self.metrics['lattice_operations']),
                'key_exchanges_count': len(self.metrics['key_exchange']),
                'microservice_calls_count': len(self.metrics['microservice_overhead']),
                'total_message_size_bytes': overhead_breakdown['communication_overhead']['total_bytes']
            }
        }
        
        return comparison_metrics
        
    def save_metrics_to_json(self, filename: Optional[str] = None) -> str:
        """Save comprehensive metrics to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fedfca_metrics_{self.actor_id}_{timestamp}.json"
            
        metrics_data = {
            'raw_metrics': dict(self.metrics),
            'comparison_analysis': self.generate_comparison_metrics(),
            'collection_metadata': {
                'collector_version': '1.0',
                'collection_start': self.start_time,
                'collection_end': time.time(),
                'actor_id': self.actor_id
            }
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(metrics_data, f, indent=2, default=str)
            
            self.logger.info(f"Metrics saved to {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"Error saving metrics: {e}")
            raise

# Usage example for responding to reviewer comments
def example_usage():
    """
    Example of how to use the metrics collector to address reviewer concerns
    about computational vs communication overhead in FedFCA
    """
    
    # Initialize collector for AGM actor
    collector = FedFCAMetricsCollector("agm_actor")
    
    # Start experiment
    collector.start_experiment_timing("FedFCA_vs_Centralized_Comparison")
    
    # Example: Record FCA lattice construction (pure computation)
    start = time.time()
    # ... FCA lattice building logic here ...
    end = time.time()
    collector.record_computation_time("lattice_construction", start, end)
    
    # Example: Record lattice transmission (communication overhead)
    start = time.time()
    lattice_size = 1024  # example size in bytes
    # ... lattice transmission via Kafka ...
    end = time.time()
    collector.record_communication_overhead("lattice_transmission", lattice_size, end - start, "kafka")
    
    # Example: Record key exchange overhead
    start = time.time()
    # ... cryptographic key exchange ...
    end = time.time()
    collector.record_key_exchange_overhead("key_generation", 2048, end - start)
    
    # Record Kafka and Redis metrics
    collector.record_kafka_metrics()
    collector.record_redis_metrics()
    
    # Stop experiment and save results
    collector.stop_experiment_timing()
    
    # Generate comprehensive metrics addressing reviewer concerns
    filename = collector.save_metrics_to_json()
    
    print(f"Comprehensive FedFCA metrics saved to: {filename}")
    print("This addresses reviewer concerns by providing:")
    print("1. Detailed computation vs communication time breakdown")
    print("2. Architecture overhead analysis (Kafka, Redis, microservices)")
    print("3. Resource usage patterns during federated execution")
    print("4. Communication overhead quantification including message sizes")
    
    return filename

if __name__ == "__main__":
    example_usage()