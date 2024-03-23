import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import redis
import logging
import json

# Connect to Redis
try:
    redis_client = redis.StrictRedis(host='datastore', port=6379, db=0)
    logging.info("Redis connection successful: %s", redis_client.client_info())
except Exception as e:
    logging.error("Error connecting to Redis: %s", e)

# Initialize Dash app
app = dash.Dash(__name__)

# Define layout
app.layout = html.Div([
    html.H1("Real-time Data Stats"),
    dcc.Interval(
        id='interval-component',
        interval=5 * 1000,  # in milliseconds
        n_intervals=0
    ),
    dcc.Graph(id='runtime-graph'),  # Add graph for runtime
    html.Div(id='f1-measure-graphs')  # Container for F1-Measure graphs
])

# Callback to update real-time stats and F1-Measure
@app.callback(
    [Output('runtime-graph', 'figure'), Output('f1-measure-graphs', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_stats(_):
    # Retrieve data stats from Redis
    data_stats = redis_client.lrange('data_stats', 0, -1)
    logging.info("Data from Redis: %s", data_stats)
    
    # Initialize lists to store data for plotting
    actors = []
    runtimes = []
    
    # Parse data and populate lists
    for data in data_stats:
        stats = eval(data.decode())  # Assuming data is stored as a stringified dictionary
        if stats != {} and stats is not None:
            actors.append(stats['Actor'])
            runtimes.append(stats['Runtime'])
            logging.info("Data extracted: %s", data)

    # Calculate average runtime
    average_runtime = sum(runtimes) / len(runtimes) if runtimes else 0
    logging.info("Average Runtime: %s", average_runtime)
    
    # Create figure for runtime graph
    figure_runtime = {
        'data': [
            {
                'x': actors,
                'y': runtimes,
                'type': 'bar',
                'name': 'Runtime',
                'marker': {'color': 'rgb(102, 178, 255)'},
                'text': runtimes,
                'textposition': 'auto'
            },
            {
                'x': actors,
                'y': [average_runtime] * len(actors),
                'type': 'line',
                'name': 'Average Runtime',
                'mode': 'lines',
                'line': {'dash': 'dash', 'color': 'blue'},
                'text': ['Average Runtime'] * len(actors),
                'textposition': 'top center'
            }
        ],
        'layout': {
            'title': 'Actor Runtimes',
            'xaxis': {'title': 'Actor'},
            'yaxis': {'title': 'Runtime (seconds)'}
        }
    }
    
    # Retrieve F1-measure data from Redis
    f1_measure_data = redis_client.lrange('data_quality', 0, -1)

    # Parse each JSON string into a dictionary
    parsed_data = [json.loads(item) for item in f1_measure_data]

    # Extract dataset IDs and F1-measures for the runtime chart
    dataset_ids = [item['Dataset_id'] for item in parsed_data]
    f1_measures = [item['F1-score'] for item in parsed_data]

    f1_measure_graphs = []
    for item in parsed_data:
        dataset_id = item['Dataset_id']
        f1_measures = item['F1-score']  # Assuming 'F1-measure' is a list in parsed data
        if not f1_measures:  # Handle cases where data might be missing
            continue

        # Create a line graph for this dataset
        figure_f1 = {
            'data': [
                {
                    'x': list(range(2, 10)),  # Privacy budgets (from 2 to 9)
                    'y': f1_measures,
                    'type': 'line',
                    'mode': 'lines+markers',
                    'name': f'Dataset {dataset_id}'
                }
            ],
            'layout': {
                'title': f'F1-Measure for Dataset {dataset_id}',
                'xaxis': {'title': 'Privacy Budget'},
                'yaxis': {'title': 'F1-Measure'}
            }
        }
        f1_measure_graphs.append(html.Div(dcc.Graph(figure=figure_f1)))

    return figure_runtime, f1_measure_graphs


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)
