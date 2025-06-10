# Dockerfile for running the dataset preparation script
# Save this as 'dataset.Dockerfile' (or any name you prefer)

# Use a Python base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY dataset_prep/requirements_dataset.txt .
RUN pip install --no-cache-dir -r requirements_dataset.txt

# Copy the dataset preparation script into the container
# Make sure your script from the artifact is saved as 'dataset_preparation_utility.py' in the same directory as this Dockerfile
COPY dataset_prep/dataset_preparation_utility.py .

# The config directory will be mounted as a volume in docker-compose

# Command to run the dataset preparation script when the container starts
# The script will output data to 'federated_data_output' inside the WORKDIR (/app)
CMD ["python", "dataset_preparation_utility.py"]
