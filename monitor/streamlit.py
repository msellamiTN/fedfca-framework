import streamlit as st
import redis
import json
import logging
import altair as alt
import pandas as pd

# Connect to Redis
try:
    redis_client = redis.StrictRedis(host='datastore', port=6379, db=0)
    logging.info("Redis connection successful: %s", redis_client.client_info())
except Exception as e:
    logging.error("Error connecting to Redis: %s", e)

# Retrieve data from Redis
data_stats_raw = redis_client.lrange('data_stats', 0, -1)
data_quality_raw = redis_client.lrange('data_quality', 0, -1)

# Parse data into structured format
data_stats = [json.loads(item.decode()) for item in data_stats_raw]
data_quality = [json.loads(item.decode()) for item in data_quality_raw]

# Filter datasets by name
dataset_names = set([data.get('Dataset_name') for data in data_stats if 'Dataset_name' in data])
selected_dataset_name = st.selectbox("Select Dataset by Name", list(dataset_names))

filtered_data_stats = [data for data in data_stats if data.get('Dataset_name') == selected_dataset_name]

# Filter datasets by privacy budget
privacy_budget_min = st.slider("Minimum Privacy Budget", min_value=0, max_value=10)
privacy_budget_max = st.slider("Maximum Privacy Budget", min_value=0, max_value=10)

filtered_data_stats = [data for data in filtered_data_stats if privacy_budget_min <= data.get('Privacy_budget') <= privacy_budget_max]

# Display filtered dataset
st.write("Filtered Dataset:")
st.write(filtered_data_stats)

# Visualization
# Bar chart for runtime using Altair
df_runtime = pd.DataFrame(filtered_data_stats)
df_runtime['Actor'] = df_runtime['Actor'].astype('category')  # Convert Actor to categorical data type
chart_runtime = alt.Chart(df_runtime).mark_bar().encode(
    x=alt.X('Actor', title='Actor'),
    y=alt.Y('Runtime', title='Runtime (seconds)'),
    color=alt.Color('Actor', legend=None),
    tooltip=['Actor', 'Runtime']
).properties(
    title='Runtime Statistics',
    width=600,
    height=400
)
st.altair_chart(chart_runtime, use_container_width=True)

# Line chart for quality metrics
selected_data_quality = [data for data in data_quality if data.get('Dataset_name') == selected_dataset_name]
if selected_data_quality:
    df_quality = pd.DataFrame(selected_data_quality)
    df_quality['Sample'] = df_quality.index + 1  # Create a numerical index starting from 1
    chart_quality = alt.Chart(df_quality).mark_line().encode(
        x=alt.X('Sample', title='Sample'),
        y=alt.Y('F1-score', title='F1-score'),
        color=alt.Color('Dataset_name', legend=alt.Legend(title='Dataset Name')),
        tooltip=['Sample', 'F1-score', 'Precision', 'Recall']
    ).properties(
        title='Quality Metrics',
        width=600,
        height=400
    )
    st.altair_chart(chart_quality, use_container_width=True)
else:
    st.warning("No quality metrics available for the selected dataset.")
