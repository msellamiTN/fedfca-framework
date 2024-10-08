# Import necessary libraries
import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from flask import Flask
import socket
import logging
import json

# Define Kafka servers
kafka_servers = 'PLAINTEXT://kafka-1:19092,PLAINTEXT://kafka-2:19093,PLAINTEXT://kafka-3:19094'

# Set logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask server
server = Flask(__name__)

# Initialize Kafka consumer
consumer = Consumer({
    'bootstrap.servers': kafka_servers,
    'group.id': 'agm_group',
    'auto.offset.reset': 'earliest',
    'client.id': socket.gethostname()
})
consumer.subscribe(['AGM'])

# Function to parse Kafka messages
def parse_message(message):
    try:
       
    # Parse message and extract relevant data
        if 'stats' in message:
            data = message['stats']
            task_id = str(data['task_id'])
            start_time = float(data['start_time'])
            end_time = float(data['end_time'])
            runtime = float(data['runtime'])
            return {'task_id': task_id, 'start_time': start_time, 'end_time': end_time, 'runtime': runtime}

    except Exception as e:
        logger.error("Error parsing message: %s", e)
    return None

# Initialize Dash app
app = dash.Dash(__name__)

# Dash layout
app.layout = html.Div([
    dcc.Graph(id='average_runtime_graph'),
    dcc.Interval(id='interval', interval=10000)  # Update interval in milliseconds
])

# Callback to update average runtime graph
@app.callback(Output('average_runtime_graph', 'figure'),
              [Input('interval', 'n_intervals')])
def update_average_runtime(n_intervals):
    logger.info("Updating average runtime graph...")
    
    # Consume messages from Kafka topic
    messages = consumer.poll(1.0)
    logger.info("No messages received from Kafka.%s",messages)
    if messages is None:
        logger.info("No messages received from Kafka.")
        return dash.no_update
    message = json.loads(messages.value().decode('utf-8'))
    valid_data =  parse_message(message)
     
    if not valid_data:
        logger.warning("No valid data found in received messages.")
        return dash.no_update
    
    # Convert data to DataFrame
    df = pd.DataFrame(valid_data)
    
    # Calculate average runtime by task_id
    avg_runtime_by_task = df.groupby('task_id')['runtime'].mean().reset_index()
    
    # Calculate global average runtime
    global_avg_runtime = df['runtime'].mean()
    
    # Create figure
    fig = {
        'data': [
            {'x': avg_runtime_by_task['task_id'], 'y': avg_runtime_by_task['runtime'], 'type': 'bar', 'name': 'Average Runtime by Task'},
            {'x': ['Global'], 'y': [global_avg_runtime], 'type': 'bar', 'name': 'Global Average Runtime'}
        ],
        'layout': {
            'title': 'Average Runtime',
            'yaxis': {'title': 'Runtime'},
            'barmode': 'group'
        }
    }
    
    logger.info("Average runtime graph updated successfully.")
    return fig

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)
