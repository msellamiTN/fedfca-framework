import logging
from confluent_kafka import Consumer, KafkaError,TopicPartition
import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import pandas as pd
import queue
 
import plotly.graph_objs as go
import plotly.subplots as sp 
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly
from dash.dependencies import Input, Output
import pandas as pd
import threading
import json
import queue
import socket
from collections import deque
class KafkaConsumerActor(threading.Thread):
    def __init__(self, kafka_servers, data_queue,que_len):
        super().__init__()
        self.topic='AGM'
        self.consumer = Consumer({
            'bootstrap.servers': kafka_servers,
            'group.id': 'agm_group',
            'auto.offset.reset': 'earliest',
            'client.id': socket.gethostname()
        })
        self.consumer.subscribe([self.topic])
        # the application needs a maximum of 180 data units
        self.data = {
            'Actor': deque(maxlen=que_len),
            'StartTime': deque(maxlen=que_len),
            'EndTime': deque(maxlen=que_len),
            'Runtime': deque(maxlen=que_len)
        }
        #self.__update_que() 
        # try:
            
        #     # download first 180 messges
        #     self.partition = TopicPartition(topic=self.topic, partition=0)
        #     low_offset, high_offset = self.consumer.get_watermark_offsets(self.partition, timeout=2)

        #     # move offset back on 180 messages
        #     if high_offset > que_len:
        #         self.partition.offset = high_offset - que_len
        #     else:
        #         self.partition.offset = low_offset

        #     # set the moved offset to consumer
        #     self.consumer.assign([self.partition])

        #     self.__update_que()   
        # except Exception as e:
        #     logging.error("Error e : %s", e)
        self.data_queue = data_queue
     # https://docs.confluent.io/current/clients/python.html#delivery-guarantees
    def __update_que(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=0.1)
                if msg is None:
                    break
                elif msg.error():
                    logging.error('error: %s',msg.error())
                    break
                else:
                    record_value = msg.value()
                    json_data = json.loads(record_value.decode('utf-8'))
                    logging.error("json_data e : %s", json_data)
                    self.data['Actor'].append(json_data['Actor'])
                    self.data['StartTime'].append(json_data['StartTime'])
                    self.data['EndTime'].append(json_data['EndTime'])
                    self.data['Runtime'].append(json_data['EndTime'])
                    logging.info('data: %s',self.data)
                    # save local offset
                    #self.partition.offset += 1          
        except Exception as e:
             
             logging.error("Error e : %s", e)
    
    def get_graph_data(self):
        self.consumer = Consumer({
            'bootstrap.servers': kafka_servers,
            'group.id': 'agm_group',
            'auto.offset.reset': 'earliest',
            'client.id': socket.gethostname()
        })
        self.consumer.subscribe(['AGM'])  

        # update low and high offsets (don't work without it)
         # self.consumer.get_watermark_offsets(self.partition, timeout=2)

        # set local offset
        #self.consumer.assign([self.partition])

        self.__update_que()

        # convert data to compatible format
        o = {key: list(value) for key, value in self.data.items()}
        return o        
        

    def get_last(self):
        if self.data is not None:
            logging.info("data:%s",self.data)
            Actor = self.data['Actor'] 
            StartTime = self.data['StartTime'] 
            EndTime = self.data['EndTime'] 
            Runtime = self.data['Runtime'] 
            return Actor, StartTime, EndTime,Runtime   
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
        self.stats_list=[]
        self.data = pd.DataFrame(columns=['Actor', 'StartTime', 'EndTime', 'Runtime'])
        logging.info("Received data queue: %s", self.data_queue)

    def run(self):
        while True:
            if not self.data_queue.empty():
                data = json.loads(self.data_queue.get())
                
                # Check if 'stats' data is present in the message
                if 'stats' in data:
                    stats = data['stats']
                    
                    # Extract the relevant statistics
                    actor_id = stats['Actor']
                    start_time = float(stats['StartTime'])
                    end_time = float(stats['EndTime'])
                    runtime = float(stats['Runtime'])
                    
                    # Create a dictionary for the statistics
                    runtime_dict = {
                        'Actor': actor_id,
                        'StartTime': start_time,
                        'EndTime': end_time,
                        'Runtime': runtime
                    }
                    
                    # Append the dictionary to the list of statistics
                    self.stats_list.append(runtime_dict)
                    
                    logging.info("Processing stats for actor: %s", actor_id)
                    
    def update_data_frame(self):
        # Convert the list of dictionaries to a DataFrame and concatenate it with the existing data
        new_data = pd.DataFrame(self.stats_list)
        self.data = pd.concat([self.data, new_data], ignore_index=True)
        
        # Clear the list of statistics for the next iteration
         #self.stats_list.clear()
        
        logging.info("Data processed: %s", self.data.head())



class DashActor:
    def __init__(self, data_queue):
        self.external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
        self.app = dash.Dash(__name__, external_stylesheets=self.external_stylesheets)
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
                kafka_servers = 'PLAINTEXT://kafka-1:19092,PLAINTEXT://kafka-2:19093,PLAINTEXT://kafka-3:19094'
                connect = KafkaConsumerActor(kafka_servers, self.data_queue, 5)
                data = connect.get_graph_data()
                df = pd.DataFrame(list(data), columns=['Actor', 'StartTime', 'EndTime', 'Runtime'])
                avg_runtime_by_task = df.groupby('Actor')['Runtime'].mean().reset_index()
                global_avg_runtime = df['Runtime'].mean()
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

    def create_graphs(self, live_update_graph):
        kafka_servers = 'PLAINTEXT://kafka-1:19092,PLAINTEXT://kafka-2:19093,PLAINTEXT://kafka-3:19094'
        connect = KafkaConsumerActor(kafka_servers, self.data_queue, 5)

        @self.app.callback(Output(live_update_graph, 'figure'),
                           [Input('interval', 'n_intervals')])
        def update_graph_live_terra(n):
            # Collect some data
            data = connect.get_graph_data()
            print('Update graph, data units:', len(data['Runtime']))

            # Create the subplots
            fig = sp.make_subplots(rows=2, cols=1, vertical_spacing=0.2)
            fig['layout']['margin'] = {'l': 30, 'r': 10, 'b': 30, 't': 10}
            fig['layout']['legend'] = {'x': 0, 'y': 1, 'xanchor': 'left'}

            # Add traces to the subplots
            fig.add_trace(go.Scatter(x=data['Actor'], y=data['Runtime'], mode='lines+markers', name='Runtime'), row=1, col=1)
            fig.add_trace(go.Scatter(x=data['Actor'], y=data['Runtime'], text=data['Runtime'], mode='lines+markers', name='Runtime vs Runtime'), row=2, col=1)

            return fig

    def run(self):
        self.app.run_server(host='0.0.0.0', port=8050, debug=True)

if __name__ == "__main__":
    kafka_servers = 'PLAINTEXT://kafka-1:19092,PLAINTEXT://kafka-2:19093,PLAINTEXT://kafka-3:19094'
    logging.basicConfig(level=logging.DEBUG)

    data_queue = queue.Queue() 
    dash_actor = DashActor(data_queue)
    
    dash_actor.run()
