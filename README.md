# FedFCA Framework

<img src="legoFCA.png" alt="FedFCA Logo" width="200" height="200" />

FedFCA is an open-source framework for Federated Formal Concept Analysis (FCA), enabling privacy-preserving, distributed data analysis. It is designed for collaborative environments where data cannot be shared directly due to privacy or regulatory constraints.

---

## Table of Contents
1. [Introduction](#introduction)
2. [Key Features](#key-features)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Usage](#usage)
6. [Docker Setup](#docker-setup)
7. [Docker Compose Setup](#docker-compose-setup)
8. [Kubernetes Setup](#kubernetes-setup)
9. [API Reference](#api-reference)
10. [Contributing](#contributing)
11. [License](#license)
12. [Acknowledgments](#acknowledgments)
13. [Contact](#contact)

---

## Introduction

Formal Concept Analysis (FCA) is a mathematical framework for data analysis, knowledge representation, and ontology engineering. FedFCA extends FCA to federated environments, where data is distributed across multiple nodes (e.g., devices or organizations) and cannot be centralized.

This framework provides tools and algorithms to perform FCA in a federated manner, ensuring data privacy and security while enabling collaborative analysis.

---

## Key Features

- **Privacy-Preserving Analysis**: Perform FCA without sharing raw data between nodes.
- **Distributed Computation**: Leverage multiple nodes to compute formal concepts collaboratively.
- **Scalable Algorithms**: Efficient algorithms designed for large-scale datasets.
- **Modular Design**: Easily extendable to support new FCA algorithms or federated learning techniques.
- **Open Source**: Fully open-source and community-driven.

---

## Installation

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)
- Docker and Docker Compose (for containerized deployment)

### Steps

1. Clone the repository:

   ```bash
   git clone https://github.com/msellamiTN/fedfca-framework.git
   cd fedfca-framework
   ```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Verify the installation:

   ```bash
   python -c "import fedfca; print(fedfca.__version__)"
   ```

---

## Configuration

The framework uses a `config.yml` file to define parameters for the federated learning process. Below is an example configuration:

```yaml
clients:
  num_clients: 5
dataset:
  url: 'https://raw.githubusercontent.com/msellamiTN/fedfca-framework/refs/heads/main/data/'
  name: 'dataset_200_10_8.data'
privacy_budget: 2
fraction: 0.4
sample_fraction: 0.5
```

### Explanation of Fields
- **clients**:
  - `num_clients`: Number of clients participating in the federation.
- **dataset**:
  - `url`: URL where the dataset is hosted.
  - `name`: Name of the dataset file.
- **privacy_budget**: Privacy budget for differential privacy mechanisms.
- **fraction**: Fraction of clients selected for training in each round.
- **sample_fraction**: Fraction of data samples used by each client during training.

---

## Usage

### Basic Example

```python
if __name__ == "__main__":
    # Define Kafka servers
    kafka_servers = 'PLAINTEXT://kafka-1:19092,PLAINTEXT://kafka-2:19093,PLAINTEXT://kafka-3:19094'

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Instantiate the ALMActor
    alm_actor = ALMActor(kafka_servers)

    # Log the actor's ID
    logging.info("actor_id: %s", alm_actor.actor_id)

    # Start the ALMActor
    alm_actor.run()

    # Log the actor's key (if applicable)
    logging.info("key: %s", alm_actor.key)
```

### Advanced Usage
For more advanced usage, such as customizing algorithms or integrating with existing federated learning frameworks, refer to the [API Reference](#api-reference).

---

## Docker Setup

The FedFCA framework can be containerized using Docker for easy deployment and reproducibility.

### Dockerfile

```dockerfile
FROM python:3.8

# Set the working directory
WORKDIR /app

# Copy requirements and configuration files
COPY requirements.txt config.yml .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Run the ALMActor script
CMD ["python", "almactor.py"]
```

### Steps to Build and Run

1. Build the Docker image:
   ```bash
   docker build -t fedfca .
   ```

2. Run the Docker container:
   ```bash
   docker run -it fedfca
   ```

---

## Docker Compose Setup

The FedFCA framework can be deployed as a multi-container application using Docker Compose. Below is the `docker-compose.yml` configuration:

```yaml
version: '3.7'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 1
    ports:
      - "2181:2181"
    networks:
      - fedfca-network

  kafka-1:
    image: confluentinc/cp-kafka:latest
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:19092,EXTERNAL://${DOCKER_HOST_IP:-127.0.0.1}:9092,DOCKER://host.docker.internal:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,DOCKER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
      KAFKA_BROKER_ID: 1
    depends_on:
      - zookeeper
    networks:
      - fedfca-network

  redis:
    image: redis
    hostname: datastore
    container_name: datastore
    ports:
      - "6379:6379"
    environment:
      - REDIS_PASSWORD=mysecred
      - REDIS_USER=fedfac
    networks:
      - fedfca-network
    volumes:
      - redis-data:/data
    restart: always

volumes:
  redis-data:

networks:
  fedfca-network:
    driver: bridge
```

### Steps to Deploy with Docker Compose

1. Navigate to the project directory:
   ```bash
   cd fedfca-framework
   ```

2. Start the services:
   ```bash
   docker-compose up -d
   ```

3. To stop the services:
   ```bash
   docker-compose down
   ```

---

## Kubernetes Setup

FedFCA can also be deployed on a Kubernetes cluster for scalable and robust deployments.

### Prerequisites
- A Kubernetes cluster (local or cloud-based)
- kubectl configured to communicate with the cluster
- Helm (optional, for easier deployment of dependencies)

### Deployment Steps

1. Create a Kubernetes namespace for FedFCA:
   ```bash
   kubectl create namespace fedfca
   ```

2. Deploy Zookeeper and Kafka:
   Use a Helm chart or a custom YAML file. For example:

   ```yaml
   apiVersion: v1
   kind: Pod
   metadata:
     name: zookeeper
     namespace: fedfca
   spec:
     containers:
     - name: zookeeper
       image: confluentinc/cp-zookeeper:latest
       ports:
       - containerPort: 2181
   ---
   apiVersion: v1
   kind: Pod
   metadata:
     name: kafka
     namespace: fedfca
   spec:
     containers:
     - name: kafka
       image: confluentinc/cp-kafka:latest
       ports:
       - containerPort: 9092
   ```

   Apply the file:
   ```bash
   kubectl apply -f zookeeper-kafka.yaml
   ```

3. Deploy the Redis datastore:
   ```yaml
   apiVersion: v1
   kind: Pod
   metadata:
     name: redis
     namespace: fedfca
   spec:
     containers:
     - name: redis
       image: redis:latest
       ports:
       - containerPort: 6379
   ```
   Apply the file:
   ```bash
   kubectl apply -f redis.yaml
   ```

4. Deploy the FedFCA application:
   Create a `fedfca-deployment.yaml`:
   ```yaml
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: fedfca
     namespace: fedfca
   spec:
     replicas: 3
     selector:
       matchLabels:
         app: fedfca
     template:
       metadata:
         labels:
           app: fedfca
       spec:
         containers:
         - name: fedfca
           image: your-docker-image:latest
           ports:
           - containerPort: 8000
   ```
   Apply the deployment:
   ```bash
   kubectl apply -f fedfca-deployment.yaml
   ```

5. Expose the application using a Service:
   ```yaml
   apiVersion: v1
   kind: Service
   metadata:
     name: fedfca-service
     namespace: fedfca
   spec:
     selector:
       app: fedfca
     ports:
       - protocol: TCP
         port: 80
         targetPort: 8000
     type: LoadBalancer
   ```
   Apply the service:
   ```bash
   kubectl apply -f fedfca-service.yaml
   ```

6. Verify the deployment:
   ```bash
   kubectl get all -n fedfca
   ```

---

## API Reference

For detailed documentation of the framework's API, including classes, methods, and parameters, refer to the [API Reference](#api-reference).

---

## Contributing

We welcome contributions from the community! If you’d like to contribute to the FedFCA Framework, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Commit your changes.
4. Submit a pull request.

Please read our [Contribution Guidelines](#) for more details.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

We would like to thank the following contributors and organizations for their support:

[List contributors or organizations here]

[Add any relevant acknowledgments]

---

## Contact

For questions, feedback, or collaboration opportunities, please contact:

**Mohamed Sellami**  
Email: msellami@example.com  
GitHub: [msellamiTN](https://github.com/msellamiTN)

---

## Star the Project

If you find this project useful, please consider giving it a ⭐️ on GitHub!
