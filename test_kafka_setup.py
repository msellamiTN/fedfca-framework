from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import json
import time
import socket

def test_kafka_connection(bootstrap_servers):
    """Test connection to Kafka broker"""
    print(f"\n=== Testing connection to Kafka at {bootstrap_servers} ===")
    
    # Test basic connectivity
    try:
        # Create a minimal producer to test connection
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname(),
            'error_cb': lambda err: print(f'Kafka error: {err}')
        }
        
        producer = Producer(conf)
        
        # Force a connection attempt by getting metadata
        producer.list_topics(timeout=5.0)
        print("✅ Successfully connected to Kafka")
        
        # List all topics
        metadata = producer.list_topics(timeout=5.0)
        print(f"\nAvailable topics: {list(metadata.topics.keys())}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        return False
    finally:
        if 'producer' in locals():
            producer.flush()
            del producer

def create_test_topic(bootstrap_servers, topic_name):
    """Create a test topic if it doesn't exist"""
    from confluent_kafka.admin import AdminClient, NewTopic
    
    print(f"\n=== Creating topic '{topic_name}' if it doesn't exist ===")
    
    conf = {'bootstrap.servers': bootstrap_servers}
    admin_client = AdminClient(conf)
    
    # Check if topic exists
    metadata = admin_client.list_topics(timeout=5.0)
    if topic_name in metadata.topics:
        print(f"Topic '{topic_name}' already exists")
        return True
    
    # Create topic
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    fs = admin_client.create_topics([new_topic])
    
    # Wait for operation to complete
    for topic, future in fs.items():
        try:
            future.result()
            print(f"✅ Successfully created topic: {topic}")
            return True
        except Exception as e:
            print(f"❌ Failed to create topic {topic}: {e}")
            return False

def send_test_message(bootstrap_servers, topic, message):
    """Send a test message to the specified topic"""
    print(f"\n=== Sending test message to topic '{topic}' ===")
    
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'test-producer',
        'message.send.max.retries': 3,
        'retry.backoff.ms': 1000
    }
    
    producer = Producer(conf)
    
    try:
        # Produce message
        producer.produce(
            topic=topic,
            key='test-key',
            value=json.dumps(message).encode('utf-8'),
            callback=lambda err, msg: (
                print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                if not err else
                print(f"❌ Failed to deliver message: {err}")
            )
        )
        
        # Wait for any outstanding messages to be delivered
        producer.flush()
        print("✅ Message sent successfully")
        return True
        
    except Exception as e:
        print(f"❌ Failed to send message: {e}")
        return False
    finally:
        producer.flush()
        del producer

def consume_messages(bootstrap_servers, topic, timeout=10):
    """Consume messages from the specified topic"""
    print(f"\n=== Consuming messages from topic '{topic}' ===")
    
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'test-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    try:
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < timeout:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                continue
                
            # Print the message
            print(f"\n📨 Received message:")
            print(f"   Topic: {msg.topic()}")
            print(f"   Partition: {msg.partition()}")
            print(f"   Offset: {msg.offset()}")
            
            if msg.key():
                print(f"   Key: {msg.key().decode('utf-8', errors='replace')}")
                
            try:
                value = json.loads(msg.value().decode('utf-8'))
                print("   Value (JSON):")
                print(json.dumps(value, indent=2))
            except:
                print(f"   Value (raw): {msg.value()}")
                
            message_count += 1
            
        print(f"\nConsumed {message_count} messages in {timeout} seconds")
        return message_count > 0
        
    except Exception as e:
        print(f"❌ Error while consuming messages: {e}")
        return False
    finally:
        consumer.close()

if __name__ == "__main__":
    KAFKA_BROKERS = "localhost:9092"  # Update if your Kafka broker is running elsewhere
    TEST_TOPIC = "test.topic"
    
    # Test connection
    if not test_kafka_connection(KAFKA_BROKERS):
        print("❌ Kafka connection test failed. Please check your Kafka setup.")
        exit(1)
    
    # Create test topic
    if not create_test_topic(KAFKA_BROKERS, TEST_TOPIC):
        print("❌ Failed to create test topic")
        exit(1)
    
    # Send test message
    test_message = {
        "timestamp": int(time.time()),
        "message": "Hello from test script!",
        "test_id": "test_123"
    }
    
    if not send_test_message(KAFKA_BROKERS, TEST_TOPIC, test_message):
        print("❌ Failed to send test message")
        exit(1)
    
    # Consume messages
    if not consume_messages(KAFKA_BROKERS, TEST_TOPIC):
        print("❌ Failed to consume messages")
        exit(1)
    
    print("\n✅ All tests completed successfully!")
