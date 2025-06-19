import time
import json
import psutil
from confluent_kafka import Producer, Consumer

# Config
KAFKA_BOOTSTRAP = 'kafka-1:9092'
TOPIC = 'federation.start'
NUM_MESSAGES = 1000
MESSAGE_SIZE_KB = 1
SEND_INTERVAL = 0.01  # 10ms
RESULT_FILE = 'results/benchmark_results.json'

def produce():
    p = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

    for i in range(NUM_MESSAGES):
        msg = {
            'id': i,
            'timestamp': time.time(),
            'data': 'x' * (MESSAGE_SIZE_KB * 1024)
        }
        p.produce(TOPIC, value=json.dumps(msg).encode('utf-8'))
        p.poll(0)
        time.sleep(SEND_INTERVAL)
    p.flush()

def consume():
    c = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'latency-test',
        'auto.offset.reset': 'earliest'
    })

    c.subscribe([TOPIC])
    latencies = []
    total_bytes = 0
    count = 0

    start_time = time.time()
    net_before = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv

    while count < NUM_MESSAGES:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue

        received_time = time.time()
        payload = json.loads(msg.value().decode('utf-8'))
        sent_time = payload['timestamp']
        latency = received_time - sent_time

        latencies.append(latency)
        total_bytes += len(msg.value())
        count += 1

    end_time = time.time()
    net_after = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
    bandwidth = (net_after - net_before) / (end_time - start_time)

    results = {
        'messages': count,
        'average_latency_sec': sum(latencies) / len(latencies),
        'max_latency_sec': max(latencies),
        'min_latency_sec': min(latencies),
        'total_bytes': total_bytes,
        'duration_sec': end_time - start_time,
        'kafka_bandwidth_bytes_per_sec': total_bytes / (end_time - start_time),
        'system_bandwidth_bytes_per_sec': bandwidth,
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
    }

    with open(RESULT_FILE, 'w') as f:
        json.dump(results, f, indent=2)

    print("✅ Results saved to", RESULT_FILE)
    c.close()

if __name__ == "__main__":
    print("⏳ Producing...")
    produce()
    print("✅ Done. Waiting before consuming...")
    time.sleep(5)
    print("⏳ Consuming...")
    consume()
