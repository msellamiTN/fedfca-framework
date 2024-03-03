from confluent_kafka import Consumer, KafkaError
import json

class AGMPiConsumer:
    def __init__(self, kafka_servers):
        self.consumer = Consumer({
            'bootstrap.servers': kafka_servers,
            'group.id': 'agm_pi_consumer_group',
            'auto.offset.reset': 'earliest'
        })
        self.pi_results = []  # Store Pi calculation results

    def subscribe(self, topics=['AGM']):
        self.consumer.subscribe(topics)

    def run(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Adjust timeout as needed

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print('%% %s [%d] reached end at offset %d\n' %
                              (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # Message is a normal message
                    self.process_message(msg)

        finally:
            # Clean up on exit
            self.consumer.close()
            # Optionally, aggregate results when the consumer is closed
            self.aggregate_pi_results()

    def process_message(self, msg):
        # Process the message
        try:
            data = json.loads(msg.value().decode('utf-8'))
            # Assuming 'pi_result' is the key for the Pi calculation result you want to aggregate
            if 'pi_result' in data:
                self.pi_results.append(data['pi_result'])
                print(f"Received Pi calculation: {data['pi_result']}")
        except json.JSONDecodeError as e:
            print(f"Error decoding message: {e}")

    def aggregate_pi_results(self):
        # Aggregate the Pi results
        if self.pi_results:
            total_pi = sum(self.pi_results)
            average_pi = total_pi / len(self.pi_results)
            print(f"Aggregated Total Pi: {total_pi}, Average Pi: {average_pi}")
        else:
            print("No Pi results to aggregate.")

if __name__ == "__main__":
    kafka_servers = 'your_kafka_servers'  # Change this to your actual Kafka server addresses
    agm_pi_consumer = AGMPiConsumer(kafka_servers)
    agm_pi_consumer.subscribe(['AGM'])  # Subscribe to the AGM topic
    agm_pi_consumer.run()  # Start the consumer
