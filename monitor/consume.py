import datetime
from confluent_kafka import Consumer, TopicPartition
import json
from collections import deque
import os
from time import sleep


class MyKafkaConnect:
    def __init__(self, topic, group, que_len=180):
        self.topic = topic
        bootstrap_servers = os.environ['BOOTSTRAP_SERVERS']

        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group,
            'enable.auto.commit': True,
        }

        # Initialize data storage
        self.data = {
            # Customize data storage fields as per your requirements
            'field1': deque(maxlen=que_len),
            'field2': deque(maxlen=que_len),
            'field3': deque(maxlen=que_len),
            'field4': deque(maxlen=que_len)
        }

        # Initialize Kafka consumer
        consumer = Consumer(self.conf)
        consumer.subscribe([self.topic])

        # Download first 180 messages
        self.partition = TopicPartition(topic=self.topic, partition=0)
        low_offset, high_offset = consumer.get_watermark_offsets(self.partition, timeout=2)

        # Move offset back by 180 messages
        if high_offset > que_len:
            self.partition.offset = high_offset - que_len
        else:
            self.partition.offset = low_offset

        # Set the moved offset to consumer
        consumer.assign([self.partition])

        self.__update_que(consumer)

    def __update_que(self, consumer):
        try:
            while True:
                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    break
                elif msg.error():
                    print('error: {}'.format(msg.error()))
                    break
                else:
                    record_value = msg.value()
                    json_data = json.loads(record_value.decode('utf-8'))

                    # Update data storage with relevant fields from Kafka messages
                    # Adjust this logic based on your message structure
                    self.data['field1'].append(json_data['field1'])
                    self.data['field2'].append(json_data['field2'])
                    self.data['field3'].append(json_data['field3'])
                    self.data['field4'].append(json_data['field4'])

                    # Save local offset
                    self.partition.offset += 1          
        finally:
            # Close down consumer to commit final offsets
            consumer.close()

    def get_data(self):
        consumer = Consumer(self.conf)
        consumer.subscribe([self.topic])  

        # Update low and high offsets (required for correctness)
        consumer.get_watermark_offsets(self.partition, timeout=2)

        # Set local offset
        consumer.assign([self.partition])

        self.__update_que(consumer)

        # Convert data to compatible format
        formatted_data = {key: list(value) for key, value in self.data.items()}
        return formatted_data

    def get_last(self):
        # Get the last values from the data storage
        last_values = {key: value[-1] for key, value in self.data.items()}
        return last_values


# Test the script
if __name__ == '__main__':
    # Initialize MyKafkaConnect with your Kafka topic and consumer group
    connect = MyKafkaConnect(topic='AGM', group='your_consumer_group')

    while True:        
        # Get the data from Kafka
        kafka_data = connect.get_data()

        # Print or process the data as needed
        print('Received data:', kafka_data)

        # Get the last values from the data storage
        last_values = connect.get_last()
        print('Last values:', last_values)

        sleep(0.2)
