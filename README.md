# Real-Time Stock Data Streaming Pipeline

![Python](https://img.shields.io/badge/Python-3.9-blue.svg) ![Docker](https://img.shields.io/badge/Docker-20.10-blue.svg) ![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4-black.svg) ![MongoDB](https://img.shields.io/badge/MongoDB-green.svg) ![yfinance](https://img.shields.io/badge/yfinance-0.2.30-orange.svg)

This project implements a robust, real-time data pipeline that fetches intraday stock market data from **Yahoo Finance**, calculates a variety of financial features, streams the data through **Apache Kafka**, and stores it in **MongoDB** for analysis or downstream consumption. The entire infrastructure is containerized using **Docker** and orchestrated with **Docker Compose**, ensuring easy setup and portability.

---

## 🏛️ Pipeline Architecture

The pipeline is designed with a decoupled, event-driven architecture to ensure scalability and resilience. The data flows through the system as follows:

1.  **Data Source (Producer)**:
    * A Python service named `yfinance-producer` runs continuously.
    * Every 60 seconds, it fetches the latest 1-minute interval stock data for a predefined list of tickers (e.g., AAPL, MSFT) using the `yfinance` library.
    * It then performs on-the-fly feature engineering, calculating key technical indicators such as RSI, ATR, moving averages, volatility, and more.
    * The producer only streams data captured during official market hours to ensure relevance.
    * Finally, it serializes the processed data into a JSON format and publishes it as a message to a specific Kafka topic.

2.  **Message Broker (Kafka)**:
    * A complete Confluent Kafka stack (including Zookeeper, Broker, and Schema Registry) acts as the central nervous system of the pipeline.
    * It receives messages from the producer and buffers them in a topic (e.g., `yfinance_features`).
    * Kafka's distributed nature allows the system to handle high throughput and provides a durable buffer, meaning consumers can go offline without data loss.

3.  **Data Sink (Consumer)**:
    * A separate Python service named `mongodb-consumer` subscribes to the Kafka topic.
    * It consumes messages in real-time as they become available.
    * Upon receiving a message, it parses the JSON data and inserts it as a new document into a specified collection within a MongoDB database.

4.  **Orchestration & Monitoring**:
    * **Docker Compose** is used to define and manage all the services (Producer, Consumer, Zookeeper, Kafka, etc.) as containers in an isolated network. This makes the entire stack easy to build, run, and scale.
    * **Confluent Control Center** is included in the stack, providing a web-based user interface to monitor the Kafka cluster, view topics, and inspect messages.



---

## ✨ Key Features

* **End-to-End Streaming**: A complete, automated pipeline from data source to data sink.
* **Real-Time Feature Engineering**: Calculates valuable financial indicators on the fly before storage.
* **Decoupled & Resilient**: The use of Kafka decouples the data producer from the consumer, allowing them to operate and scale independently.
* **Containerized & Portable**: Fully containerized with Docker, enabling one-command setup on any machine with Docker installed.
* **Configurable**: Easily change stock tickers, Kafka topics, and MongoDB connection details through environment variables.
* **Monitoring Ready**: Includes Confluent Control Center for easy management and monitoring of the Kafka cluster.

---

## 🛠️ Prerequisites

Before you begin, ensure you have the following installed on your local machine:
* [Docker](https://www.docker.com/products/docker-desktop/)
* [Docker Compose](https://docs.docker.com/compose/install/) (typically included with Docker Desktop)
* [Git](https://git-scm.com/downloads)

---

## 🚀 Setup and Installation

Follow these steps to get the pipeline up and running.

### Step 1: Clone the Repository

Open your terminal and clone this project repository to your local machine.

```bash
git clone <your-repository-url>
cd <repository-directory>

```
### Step 2: Configure Environment Variables

The consumer service needs a connection string to access your MongoDB instance.

1.  Create a file named `.env` in the root directory of the project.
2.  Add your MongoDB connection URI to this file. It should look like this:

    ```env
    # .env
    MONGO_URI="mongodb+srv://<user>:<password>@<your-cluster-url>/<db-name>?retryWrites=true&w=majority"
    ```

    > **Note**: Replace the placeholders with your actual MongoDB credentials and cluster information. This URI is securely passed to the consumer container at runtime.

### Step 3: Build and Run the Pipeline

With Docker running, execute the following command from the root of the project directory:

```bash
docker-compose up -d --build
