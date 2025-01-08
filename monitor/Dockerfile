# Stage 1: Build stage
FROM python:3.8-slim AS build-stage

# Set the working directory in the container
WORKDIR /app

# Copy only the requirements file to the container
COPY requirements.txt /app/
COPY . /app
# Install the requirements without caching
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Production stage
#FROM build-stage

# Set the working directory in the container
#WORKDIR /app

 
# Copy the current directory contents into the container at /app
#COPY . /app
# Expose the port the app runs on
EXPOSE 8051

# Run the Python script when the container launches
#CMD ["python", "monitor.py"]
# Run the application
CMD ["streamlit", "run", "streamlit.py", "--server.port=8051", "--server.address=0.0.0.0"]