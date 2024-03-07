import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import redis
import logging

# Connect to Redis
try:
    redis_client = redis.StrictRedis(host='datastore', port=6379, db=0)
except Exception as e:
    logging.error("Error connecting to Redis: %s", e)

# Initialize Dash app
app = dash.Dash(__name__)

 

# Define layout
app.layout = html.Div([
    html.H1("Real-time Data Stats"),
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # in milliseconds
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
    start_times = []
    end_times = []
    runtimes = []
    
    # Parse data and populate lists
    for data in data_stats:
        stats = eval(data.decode())  # Assuming data is stored as a stringified dictionary
        actors.append(stats['Actor'])
        start_times.append(stats['StartTime'])
        end_times.append(stats['EndTime'])
        runtimes.append(stats['Runtime'])
    
    # Create a bar chart for each actor's runtime
     # Create a bar chart for each actor's runtime
    # Create a bar chart for each actor's runtime
    # figure = {
    #     'data': [
    #         {
    #             'x': actors,
    #             'y': runtimes,
    #             'type': 'bar',
    #             'name': 'Runtime',
    #             'marker': {
    #                 'color': ['rgb(255, 102, 102)', 'rgb(102, 255, 178)', 'rgb(255, 178, 102)']  # Add color codes for each bar
    #             },
    #             'text': runtimes,  # Display runtime values on top of each bar
    #             'textposition': 'auto',  # Automatically position the text on top of each bar
    #         }
    #     ],
    #     'layout': {
    #         'title': 'Actor Runtimes',
    #         'xaxis': {'title': 'Actor'},
    #         'yaxis': {'title': 'Runtime (seconds)'}
    #     }
    # }
    # Calculate average runtime
    average_runtime = sum(runtimes) / len(runtimes)

    # Create a bar chart for each actor's runtime
    figure = {
        'data': [
            {
                'x': actors,
                'y': runtimes,
                'type': 'bar',
                'name': 'Runtime',
                'marker': {
                    'color': ['rgb(255, 102, 102)', 'rgb(102, 255, 178)', 'rgb(255, 178, 102)']  # Add color codes for each bar
                },
                'text': runtimes,  # Display runtime values on top of each bar
                'textposition': 'auto',  # Automatically position the text on top of each bar
            },
            {
                'x': actors,
                'y': [average_runtime] * len(actors),  # Create a list of the same average value for each actor
                'type': 'line',
                'name': 'Average Runtime',
                'mode': 'lines',
                'line': {'dash': 'dash', 'color': 'blue'},  # Set line properties (dashed and blue)
                'text': ['Average Runtime'] * len(actors),  # Display 'Average Runtime' label for each data point
                'textposition': 'top center',  # Position label at the top center of each data point
            }
        ],
        'layout': {
            'title': 'Actor Runtimes',
            'xaxis': {'title': 'Actor'},
            'yaxis': {'title': 'Runtime (seconds)'}
        }
    }
        # Calculate F1-Measure for each dataset ID
    f1_measure_data = [
        {'dataset_id': 'dataset_1', 'f1_measure': 0.85},
        {'dataset_id': 'dataset_2', 'f1_measure': 0.91},
        {'dataset_id': 'dataset_3', 'f1_measure': 0.78}
    ]

    dataset_ids = [item['dataset_id'] for item in f1_measure_data]
    f1_measures = [item['f1_measure'] for item in f1_measure_data]

    # Create a bar chart for F1-Measure of each dataset ID
    figure_runtime = {
        'data': [
            {
                'x': dataset_ids,
                'y': f1_measures,
                'type': 'bar',
                'name': 'F1-Measure',
                'marker': {'color': 'rgb(102, 178, 255)'}  # Set color for bars
            }
        ],
        'layout': {
            'title': 'F1-Measure for Dataset IDs',
            'xaxis': {'title': 'Dataset ID'},
            'yaxis': {'title': 'F1-Measure'}
        }
    }
    # F1-Measure data for each dataset ID (replace with your implementation)
    f1_measure_data = {
        'dataset_1': [0.85, 0.87, 0.80, 0.78, 0.75, 0.72, 0.70, 0.68, 0.65],
        'dataset_2': [0.91, 0.89, 0.87, 0.84, 0.82, 0.80, 0.78, 0.75, 0.72],
        'dataset_3': [0.78, 0.80, 0.75, 0.72, 0.70, 0.68, 0.65, 0.62, 0.60]
    }

    # Create a figure for each dataset ID
    f1_measure_graphs = []
    for dataset_id, f1_measures in f1_measure_data.items():
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
