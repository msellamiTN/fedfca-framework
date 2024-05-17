import streamlit as st
import redis
import json

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Define the Streamlit app title and layout
st.title("Real-time Data Stats")

# Update the real-time stats and F1-Measure
@st.cache(ttl=5)  # Cache the function output for 5 seconds
def update_stats():
    # Retrieve data stats from Redis
    data_stats = [json.loads(item) for item in redis_client.lrange('data_stats', 0, -1)]
    
    # Initialize lists to store data for plotting
    actors = []
    runtimes = []

    # Parse data and populate lists
    for data in data_stats:
        if data != {} and data is not None:
            actors.append(data['Actor'])
            runtimes.append(data['Runtime'])

    # Calculate average runtime
    average_runtime = sum(runtimes) / len(runtimes) if runtimes else 0

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
    f1_measure_data = [json.loads(item) for item in redis_client.lrange('data_quality', 0, -1)]

    # Extract dataset IDs and F1-measures for the runtime chart
    f1_measure_graphs = []
    for data in f1_measure_data:
        dataset_id = data['Dataset_id']
        f1_measures = data['F1-score'] if 'F1-score' in data else []
        
        # Create a line graph for this dataset
        if f1_measures:
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
            f1_measure_graphs.append(figure_f1)

    return figure_runtime, f1_measure_graphs

# Retrieve the updated stats and F1-Measure
runtime_stats, f1_measure_graphs = update_stats()

# Display the runtime graph
st.subheader("Actor Runtimes")
st.plotly_chart(runtime_stats)

# Display the F1-Measure graphs
for graph in f1_measure_graphs:
    st.subheader(graph['layout']['title'])
    st.plotly_chart(graph)

# Display the Streamlit app
st.write("Real-time Data Stats are updated every 5 seconds.")
