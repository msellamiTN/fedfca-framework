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

# Define the function to get unique dataset IDs
def get_unique_dataset_ids():
    """Extracts unique dataset IDs from data_stats entries."""
    dataset_ids = set()
    data_stats = redis_client.lrange('data_stats', 0, -1)
    for data in data_stats:
        stats = json.loads(data.decode())
        if 'Dataset_id' in stats:
            dataset_ids.add(stats['Dataset_id'])
    return list(dataset_ids)

# Initialize Dash app
app = dash.Dash(__name__)

# Define layout with dropdown for dataset selection
app.layout = html.Div([
    html.H1("Real-time Data Stats and Quality"),
    dcc.Dropdown(
        id='dataset-dropdown',
        options=[{'label': f'Dataset {id}', 'value': id} for id in get_unique_dataset_ids()],
        value=get_unique_dataset_ids()[0]  # Pre-select the first dataset
    ),
    dcc.Interval(
        id='interval-component',
        interval=5 * 1000,  # in milliseconds
        n_intervals=0
    ),
    dcc.Graph(id='runtime-graph'),  # Graph for runtime by actor
    dcc.Graph(id='f1-measure-graph')  # Graph for F1-measure by varying privacy budget
])

# Filter data by dataset ID
def filter_data_stats_by_dataset(dataset_id):
    data_stats = redis_client.lrange('data_stats', 0, -1)
    return [data for data in data_stats if json.loads(data.decode()).get('Dataset_id') == dataset_id]

def filter_data_quality_by_dataset(dataset_id):
    data_quality = redis_client.lrange('data_quality', 0, -1)
    return [data for data in data_quality if json.loads(data.decode()).get('Dataset_id') == dataset_id]

# Callback to update runtime graph for selected dataset
@app.callback(
    Output('runtime-graph', 'figure'),
    [Input('interval-component', 'n_intervals'), Input('dataset-dropdown', 'value')]
)
def update_runtime_graph(_n_intervals, selected_dataset_id):
    # Retrieve data for the chosen dataset
    data_stats = filter_data_stats_by_dataset(selected_dataset_id)

    # Process data for plotting
    actors = []
    runtimes = []
    for data in data_stats:
        stats = json.loads(data.decode())
        if stats != {} and stats is not None:
            actors.append(stats['Actor'])
            runtimes.append(stats['Runtime'])

    # Calculate average runtime (if data exists)
    average_runtime = sum(runtimes) / len(runtimes) if runtimes else 0

    # Create figure for runtime graph
    figure = {
        'data': [
            {
                'x': actors,
                'y': runtimes,
                'type': 'bar',
                'name': 'Runtime (seconds)',
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
            'title': f'Dataset {selected_dataset_id} Runtime by Actor',
            'xaxis': {'title': 'Actor'},
            'yaxis': {'title': 'Runtime (seconds)'}
        }
    }

    return figure

# Callback to update F1-measure graph for selected dataset
@app.callback(
    Output('f1-measure-graph', 'figure'),
    [Input('interval-component', 'n_intervals'), Input('dataset-dropdown', 'value')]
)
def update_f1_measure_graph(_n_intervals, selected_dataset_id):
    # Retrieve data for the chosen dataset
    data_quality = filter_data_quality_by_dataset(selected_dataset_id)

    # Process data for plotting
    privacy_budgets = []
    f1_measures = []
    for data in data_quality:
        data = json.loads(data.decode())
        privacy_budgets.append(data['Privacy_budget'])
        f1_measures.append(data['F1-score'])

    # Create figure for F1-measure graph
    figure = {
        'data': [
            {
                'x': privacy_budgets,
                'y': f1_measures,
                'type': 'line',
                'name': 'F1-Measure',
                'mode': 'lines+markers',
                'line': {'dash': 'dash', 'color': 'orange'},
                'text': f1_measures,
                'textposition': 'top center'
            }
        ],
        'layout': {
            'title': f'Dataset {selected_dataset_id} F1-Measure by Privacy Budget',
            'xaxis': {'title': 'Privacy Budget'},
            'yaxis': {'title': 'F1-Measure'}
        }
    }

    return figure

# Run the Dash app
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)
