from confluent_kafka import Producer
import json
import time

def delivery_report(err, msg):
    """Called once for each message to indicate delivery result."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def send_test_message(topic, message):
    # Producer configuration
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'test-producer'
    }

    # Create Producer instance
    p = Producer(**conf)

    try:
        # Produce message
        p.produce(topic, 
                 key='test-key',
                 value=json.dumps(message).encode('utf-8'),
                 callback=delivery_report)
        
        # Wait for any outstanding messages to be delivered and delivery reports received
        p.flush()
        print(f"Sent test message to {topic}")
        
    except Exception as e:
        print(f"Failed to send message: {e}")
    finally:
        # Clean up
        p.flush()
        del p

if __name__ == "__main__":
    # Test message for provider.config
    test_message = {
        "action": "configure",
        "provider_id": "test-provider-1",
        "federation_id": "fed-123",
        "config": {
            "dataset_path": "/data/sample.csv",
            "threshold": 0.5,
            "parameters": {
                "batch_size": 32,
                "epochs": 10
            }
        },
        "timestamp": int(time.time())
    }
    
    # Send the test message
    send_test_message("provider.config", test_message)
