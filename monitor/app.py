import logging
from confluent_kafka import Consumer, KafkaError
import dash
from dash.dependencies import Input, Output
from dash import dcc, html
import pandas as pd
import threading
import json
import queue
import socket

class KafkaConsumerActor(threading.Thread):
    def __init__(self, kafka_servers, data_queue):
        super().__init__()
        self.consumer = Consumer({
            'bootstrap.servers': kafka_servers,
            'group.id': 'agm_group',
            'auto.offset.reset': 'earliest',
            'client.id': socket.gethostname()
        })
        self.consumer.subscribe(['AGM'])
        self.data_queue = data_queue

    def run(self):
        while True:
            msg = self.consumer.poll(1.0)
           
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error("Consumer error: %s", msg.error())
                    continue
            if 'stats' in msg.value().decode('utf-8'):
                self.data_queue.put(msg.value().decode('utf-8'))
                logging.info("message:%s",msg.value().decode('utf-8'))

class DataProcessingActor(threading.Thread):
    def __init__(self, data_queue):
        super().__init__()
        self.data_queue = data_queue
        self.stats_list=None
        self.data = pd.DataFrame(columns=['Actor', 'StartTime', 'EndTime', 'Runtime'])
        logging.info("Received data queue: %s", self.data_queue)

    def run(self):
        while True:
            if not self.data_queue.empty():
                data = json.loads(self.data_queue.get())
                
                # Check if 'stats' data is present in the message
                if 'stats' in data:
                    stats_data = data['stats']
                    
                    # Iterate over each actor ID and its associated statistics
                    for actor_id, stats in stats_data.items():
                        logging.info("Processing stats for actor: %s", actor_id)
                        
                        # Extract the relevant statistics for the actor
                        start_time = float(stats['StartTime'])
                        end_time = float(stats['EndTime'])
                        runtime = float(stats['Runtime'])
                        
                        # Create a dictionary for the statistics
                        runtime_dict = {
                            'Actor': str(actor_id),
                            'StartTime': start_time,
                            'EndTime': end_time,
                            'Runtime': runtime
                        }
                        
                        # Append the dictionary to the list of statistics
                        self.stats_list.append(runtime_dict)
                        
                    # After processing all statistics for this message, update the DataFrame
                    self.update_data_frame()
                    
    def update_data_frame(self):
        # Convert the list of dictionaries to a DataFrame and concatenate it with the existing data
        new_data = pd.DataFrame(self.stats_list)
        self.data = pd.concat([self.data, new_data], ignore_index=True)
        
        # Clear the list of statistics for the next iteration
        self.stats_list.clear()
        
        logging.info("Data processed: %s", self.data.head())

class DashActor:
    def __init__(self, data_queue):
        self.app = dash.Dash(__name__)
        self.data_queue = data_queue
        self.init_layout()

    def init_layout(self):
        self.app.layout = html.Div([
            dcc.Graph(id='average_runtime_graph'),
            dcc.Interval(id='interval', interval=10000)  # Update interval in milliseconds
        ])

        @self.app.callback(Output('average_runtime_graph', 'figure'),
                           [Input('interval', 'n_intervals')])
        def update_average_runtime(n_intervals):
            if not self.data_queue.empty():
                df = pd.DataFrame(list(self.data_queue.queue), columns=['Actor', 'StartTime', 'EndTime', 'Runtime'])
                avg_runtime_by_task = df.groupby('Actor')['Runtime'].mean().reset_index()
                global_avg_runtime = df['runtime'].mean()
                logging.info("Global average runtime: %s", global_avg_runtime)
                fig = {
                    'data': [
                        {'x': avg_runtime_by_task['Actor'], 'y': avg_runtime_by_task['Runtime'], 'type': 'bar', 'name': 'Average Runtime by Task'},
                        {'x': ['Global'], 'y': [global_avg_runtime], 'type': 'bar', 'name': 'Global Average Runtime'}
                    ],
                    'layout': {
                        'title': 'Average Runtime',
                        'yaxis': {'title': 'Runtime'},
                        'barmode': 'group'
                    }
                }
                return fig
            else:
                return dash.no_update

    def run(self):
        self.app.run_server(host='0.0.0.0', port=8050, debug=True)

if __name__ == "__main__":
    kafka_servers = 'PLAINTEXT://kafka-1:19092,PLAINTEXT://kafka-2:19093,PLAINTEXT://kafka-3:19094'
    logging.basicConfig(level=logging.DEBUG)

    data_queue = queue.Queue()
    kafka_consumer_actor = KafkaConsumerActor(kafka_servers, data_queue)
    data_processing_actor = DataProcessingActor(data_queue)
    dash_actor = DashActor(data_queue)

    kafka_consumer_actor.start()
    data_processing_actor.start()
    dash_actor.run()
