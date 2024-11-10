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
    dcc.Graph(id='combined-graph'),  # Combined graph for runtime and F1-measure
])

# Filter data by dataset ID
def filter_data_stats_by_dataset(dataset_id):
    data_stats = redis_client.lrange('data_stats', 0, -1)
    return [data for data in data_stats if json.loads(data.decode()).get('Dataset_id') == dataset_id]

def filter_data_quality_by_dataset(dataset_id):
    data_quality = redis_client.lrange('data_quality', 0, -1)
    return [data for data in data_quality if json.loads(data.decode()).get('Dataset_id') == dataset_id]

# Callback to update combined graph for selected dataset
@app.callback(
    Output('combined-graph', 'figure'),
    [Input('interval-component', 'n_intervals'), Input('dataset-dropdown', 'value')]
)
def update_graph(_n_intervals, selected_dataset_id):
    # Retrieve data for the chosen dataset
    data_stats = filter_data_stats_by_dataset(selected_dataset_id)
    data_quality = filter_data_quality_by_dataset(selected_dataset_id)

    # Process data for plotting
    actors = []
    runtimes = []
    privacy_budgets = []
    f1_measures = []
    for data in data_stats:
        stats = json.loads(data.decode())
        if stats != {} and stats is not None:
            actors.append(stats['Actor'])
            runtimes.append(stats['Runtime'])
    for data in data_quality:
        data = json.loads(data.decode())
        privacy_budgets.append(data['Privacy_budget'])
        f1_measures.append(data['F1-score'])

    # Calculate average runtime (if data exists)
    average_runtime = sum(runtimes) / len(runtimes) if runtimes else 0

    # Create combined figure
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
                'x': privacy_budgets,
                'y': f1_measures,
                'type': 'line',
                'name': 'F1-Measure',
                'mode': 'lines+markers',
                'yaxis': 'y2',
                'line': {'dash': 'dash', 'color': 'orange'},
                'text': f1_measures,
                'textposition': 'top center'
            }
        ],
        'layout': {
            'title': f'Dataset {selected_dataset_id} Stats',
            'xaxis': {'title': 'Actor'},
            'yaxis': {'title': 'Runtime (seconds)', 'showgrid': False},
            'yaxis2': {'title': 'F1-Measure', 'overlaying': 'y', 'anchor': 'x'}  # Align F1-measure axis
        }
    }

    return figure

# Run the Dash app
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)
