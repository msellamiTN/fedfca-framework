# FedFCA Framework

![FedFCA Logo](#) <!-- Add a logo if available -->

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
8. [API Reference](#api-reference)
9. [Contributing](#contributing)
10. [License](#license)
11. [Acknowledgments](#acknowledgments)
12. [Contact](#contact)

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

  # Add additional services here

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
