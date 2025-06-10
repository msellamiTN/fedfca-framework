from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import logging
import argparse
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_setup')

DEFAULT_TOPICS = [
    'provider.config',
    'federation.start',
    'federation.complete',
    'model.updates',
    'training.status',
    'lattice.result'
]

def setup_kafka_topics(bootstrap_servers: str = 'kafka-1:9092', 
                      topics: Optional[List[str]] = None,
                      num_partitions: int = 1,
                      replication_factor: int = 1,
                      timeout: float = 10.0) -> bool:
    """
    Set up Kafka topics if they don't exist.
    
    Args:
        bootstrap_servers: Comma-separated list of Kafka brokers
        topics: List of topic names to create (uses DEFAULT_TOPICS if None)
        num_partitions: Number of partitions for new topics
        replication_factor: Replication factor for new topics
        timeout: Timeout in seconds for Kafka operations
        
    Returns:
        bool: True if all topics were created successfully, False otherwise
    """
    if topics is None:
        topics = DEFAULT_TOPICS
    
    # Remove duplicates while preserving order
    topics = list(dict.fromkeys(topics))
    
    logger.info(f"Setting up Kafka topics: {', '.join(topics)}")
    logger.info(f"Connecting to Kafka at: {bootstrap_servers}")
    
    try:
        # Initialize admin client
        admin_client = AdminClient({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'kafka_setup_script',
            'socket.timeout.ms': int(timeout * 1000),
            'metadata.request.timeout.ms': int(timeout * 1000)
        })
        
        # Get existing topics
        try:
            existing_topics = admin_client.list_topics(timeout=timeout).topics
            logger.info(f"Found {len(existing_topics)} existing topics")
        except Exception as e:
            logger.error(f"Failed to list existing topics: {e}")
            return False
        
        # Create new topics that don't exist
        new_topics = []
        for topic in topics:
            if topic not in existing_topics:
                new_topics.append(
                    NewTopic(
                        topic,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor
                    )
                )
        
        if new_topics:
            try:
                # Create topics
                fs = admin_client.create_topics(new_topics, operation_timeout=timeout)
                
                # Wait for each topic creation to complete
                for topic, future in fs.items():
                    try:
                        future.result()  # Wait for the topic to be created
                        logger.info(f"Created topic: {topic}")
                    except Exception as e:
                        if "already exists" in str(e):
                            logger.info(f"Topic {topic} already exists")
                        else:
                            logger.error(f"Failed to create topic {topic}: {e}")
                            return False
            except KafkaException as ke:
                if "already exists" in str(ke):
                    logger.info("Some or all topics already exist")
                else:
                    logger.error(f"Error creating topics: {ke}")
                    return False
            except Exception as e:
                logger.error(f"Unexpected error creating topics: {e}")
                return False
        
        # Verify all required topics exist
        try:
            existing_topics = admin_client.list_topics(timeout=timeout).topics
            missing_topics = [topic for topic in topics if topic not in existing_topics]
            
            if missing_topics:
                logger.error(f"Failed to create the following topics: {', '.join(missing_topics)}")
                return False
            
            logger.info("All required topics exist and are accessible")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying topics: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error setting up Kafka topics: {e}")
        return False

def parse_arguments():
    parser = argparse.ArgumentParser(description='Set up Kafka topics')
    parser.add_argument(
        '--bootstrap-servers',
        default='kafka-1:9092',
        help='Comma-separated list of Kafka bootstrap servers (default: kafka-1:9092)'
    )
    parser.add_argument(
        '--topics',
        nargs='+',
        help='List of topics to create (default: built-in list)'
    )
    parser.add_argument(
        '--partitions',
        type=int,
        default=1,
        help='Number of partitions per topic (default: 1)'
    )
    parser.add_argument(
        '--replication-factor',
        type=int,
        default=1,
        help='Replication factor for topics (default: 1)'
    )
    parser.add_argument(
        '--timeout',
        type=float,
        default=10.0,
        help='Timeout in seconds for Kafka operations (default: 10.0)'
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    success = setup_kafka_topics(
        bootstrap_servers=args.bootstrap_servers,
        topics=args.topics,
        num_partitions=args.partitions,
        replication_factor=args.replication_factor,
        timeout=args.timeout
    )
    exit(0 if success else 1)
