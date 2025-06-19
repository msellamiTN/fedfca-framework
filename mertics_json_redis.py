import redis
import json
import csv
from datetime import datetime
import sys

class FedFCAMetricsExtractor:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0, redis_password=None):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            print(f"✅ Connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError:
            print(f"❌ Could not connect to Redis at {redis_host}:{redis_port}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Redis connection error: {e}")
            sys.exit(1)

    def get_fedfca_keys(self, pattern="fedfca:metrics:*"):
        """Get all FedFCA metrics keys from Redis"""
        try:
            keys = self.redis_client.keys(pattern)
            return keys
        except Exception as e:
            print(f"❌ Error getting keys: {e}")
            return []

    def read_metrics_from_key(self, key):
        """Read and parse metrics from a specific Redis key"""
        try:
            raw_data = self.redis_client.get(key)
            if raw_data:
                data = json.loads(raw_data)
                return data
            else:
                print(f"⚠️ No data found for key: {key}")
                return None
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error for key {key}: {e}")
            return None
        except Exception as e:
            print(f"❌ Error reading key {key}: {e}")
            return None

    def extract_metrics(self, data):
        """Extract relevant metrics from the JSON data"""
        extracted_metrics = []
        
        if not data:
            return extracted_metrics
            
        for entry in data:
            # Get metrics from nested structure
            if 'metrics' in entry:
                m = entry['metrics']
            else:
                m = entry
            
            # Convert timestamp to readable format
            timestamp = entry.get('timestamp', 0)
            readable_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') if timestamp else 'N/A'
            
            extracted_metrics.append({
                'timestamp': timestamp,
                'readable_timestamp': readable_time,
                'provider_id': entry.get('provider_id') or m.get('provider_id'),
                'federation_id': entry.get('federation_id') or m.get('federation_id'),
                'computation_time': m.get('computation_time'),
                'total_time': m.get('total_time'),
                'kafka_latency_ms': m.get('kafka_latency'),
                'peak_memory_mb': m.get('peak_memory_mb'),
                'cpu_percent': m.get('cpu_percent'),
                'bytes_transmitted': m.get('bytes_transmitted'),
                'dataset_name': m.get('dataset_name'),
                'status': m.get('status'),
                'lattice_size': m.get('lattice_size'),
                'encryption_time': m.get('encryption_time'),
                'serialization_time': m.get('serialization_time'),
                'network_time': m.get('network_time'),
                'service_discovery_time': m.get('service_discovery_time'),
                'key_exchange_time': m.get('key_exchange_time'),
                'key_verification_time': m.get('key_verification_time'),
                'message_count': m.get('message_count'),
                'retry_count': m.get('retry_count'),
                'strategy': m.get('strategy'),
                'threshold': m.get('threshold'),
                'context_sensitivity': m.get('context_sensitivity'),
                'node_count': m.get('node_count'),
                'federation_start_time': m.get('federation_start_time')
            })
        
        return extracted_metrics

    def save_metrics(self, metrics, output_prefix='fedfca_metrics'):
        """Save metrics to JSON and CSV files"""
        if not metrics:
            print("⚠️ No metrics to save")
            return
        
        # Save as JSON
        json_filename = f'{output_prefix}.json'
        with open(json_filename, 'w') as jf:
            json.dump(metrics, jf, indent=4)
        
        # Save as CSV
        csv_filename = f'{output_prefix}.csv'
        with open(csv_filename, 'w', newline='', encoding='utf-8') as cf:
            if metrics:
                writer = csv.DictWriter(cf, fieldnames=metrics[0].keys())
                writer.writeheader()
                writer.writerows(metrics)
        
        print(f"✅ Metrics saved to '{json_filename}' and '{csv_filename}'")
        print(f"📊 Total records processed: {len(metrics)}")

    def process_all_fedfca_metrics(self, key_pattern="fedfca:metrics:*", output_prefix='fedfca_metrics_summary'):
        """Process all FedFCA metrics from Redis"""
        print(f"🔍 Searching for keys matching pattern: {key_pattern}")
        keys = self.get_fedfca_keys(key_pattern)
        
        if not keys:
            print("❌ No FedFCA metrics keys found")
            return
        
        print(f"📋 Found {len(keys)} keys:")
        for key in keys:
            print(f"  - {key}")
        
        all_metrics = []
        
        for key in keys:
            print(f"\n📖 Processing key: {key}")
            data = self.read_metrics_from_key(key)
            if data:
                metrics = self.extract_metrics(data)
                all_metrics.extend(metrics)
                print(f"  ✅ Extracted {len(metrics)} records")
            else:
                print(f"  ⚠️ No data extracted from {key}")
        
        if all_metrics:
            self.save_metrics(all_metrics, output_prefix)
            self.print_summary(all_metrics)
        else:
            print("❌ No metrics extracted from any keys")

    def process_specific_key(self, key, output_prefix=None):
        """Process metrics from a specific Redis key"""
        if not output_prefix:
            # Generate output prefix from key name
            output_prefix = key.replace(':', '_').replace('fedfca_metrics_', '')
        
        print(f"📖 Processing specific key: {key}")
        data = self.read_metrics_from_key(key)
        
        if data:
            metrics = self.extract_metrics(data)
            if metrics:
                self.save_metrics(metrics, output_prefix)
                self.print_summary(metrics)
            else:
                print("❌ No metrics extracted")
        else:
            print("❌ No data found or parsing failed")

    def print_summary(self, metrics):
        """Print a summary of the extracted metrics"""
        if not metrics:
            return
        
        print(f"\n📊 METRICS SUMMARY:")
        print(f"Total records: {len(metrics)}")
        
        # Get unique providers and federations
        providers = set(m['provider_id'] for m in metrics if m['provider_id'])
        federations = set(m['federation_id'] for m in metrics if m['federation_id'])
        
        print(f"Unique providers: {len(providers)} - {', '.join(sorted(providers))}")
        print(f"Unique federations: {len(federations)} - {', '.join(sorted(federations))}")
        
        # Calculate some basic stats
        comp_times = [m['computation_time'] for m in metrics if m['computation_time'] is not None]
        if comp_times:
            print(f"Computation time - Min: {min(comp_times):.2f}s, Max: {max(comp_times):.2f}s, Avg: {sum(comp_times)/len(comp_times):.2f}s")
        
        mem_usage = [m['peak_memory_mb'] for m in metrics if m['peak_memory_mb'] is not None]
        if mem_usage:
            print(f"Peak memory - Min: {min(mem_usage):.1f}MB, Max: {max(mem_usage):.1f}MB, Avg: {sum(mem_usage)/len(mem_usage):.1f}MB")

def main():
    # Configuration - modify these as needed
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = None  # Set if your Redis requires authentication
    
    # Initialize extractor
    extractor = FedFCAMetricsExtractor(
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
        redis_db=REDIS_DB,
        redis_password=REDIS_PASSWORD
    )
    
    print("🚀 FedFCA Metrics Extractor")
    print("=" * 50)
    
    # Option 1: Process all FedFCA metrics
    print("\n1️⃣ Processing all FedFCA metrics...")
    extractor.process_all_fedfca_metrics()
    
    # Option 2: Process specific key (uncomment and modify as needed)
    # print("\n2️⃣ Processing specific key...")
    # extractor.process_specific_key('fedfca:metrics:fed_6v1vgjqc', 'fed_6v1vgjqc_metrics')
    
    print("\n✅ Processing complete!")

if __name__ == "__main__":
    main()