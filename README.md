# Real-Time News Analysis System

This project implements a real-time dynamic change analysis system for news topics. It consists of several modules that work together to simulate, extract, transform, load (ETL), store, analyze, and visualize news data.

## Project Structure

The project is organized into the following main directories:

- **api**: Contains the backend application built with Flask.
  - **src**: Source code for the backend.
    - **app.py**: Main entry point for the backend application.
    - **etl**: Module for ETL processes.
      - **etl_pipeline.py**: Implementation of the ETL pipeline using the Flume framework.
    - **analysis**: Module for data analysis.
      - **analyzer.py**: Provides analysis functionalities using Spark Streaming.
    - **storage**: Module for database interactions.
      - **db.py**: Handles data storage and retrieval.
    - **simulation**: Module for simulating news exposure logs.
      - **simulator.py**: Generates time series logs for news exposure and clickstream data.
  - **requirements.txt**: Lists the Python dependencies required for the backend application.
  - **README.md**: Documentation for the backend API.

- **frontend**: Contains the frontend application built with React.
  - **src**: Source code for the frontend.
    - **App.js**: Main entry point for the frontend application.
    - **components**: Contains React components.
      - **NewsVisualization.js**: Component for visualizing news data.
    - **services**: Contains service files for API interactions.
      - **api.js**: Functions to interact with the backend API.
  - **package.json**: Configuration file for the frontend application.
  - **README.md**: Documentation for the frontend application.

- **docker-compose.yml**: Defines the services required for the application, including the backend, frontend, Kafka, Zookeeper, and PostgreSQL database.

- **.gitignore**: Specifies files and directories to be ignored by Git.

## Getting Started

To run the application, follow these steps:

1. **Clone the repository**:
   ```
   git clone <repository-url>
   cd realtime-news-bi-analysis
   ```

2. **Set up the backend**:
   - Navigate to the `api` directory.
   - Install the required Python packages:
     ```
     pip install -r requirements.txt
     ```

3. **Set up the frontend**:
   - Navigate to the `frontend` directory.
   - Install the required Node.js packages:
     ```
     npm install
     ```

4. **Run the application**:
   - Use Docker Compose to start all services:
     ```
     docker-compose up
     ```
   - If Flume image doesn't work, start Flume locally (with powershell)
     ```
     $env:JAVA_OPTS="-Dflume.root.logger=INFO,console"
     flume-ng agent -n agent -conf ./api/src/flume -conf-file ./api/src/flume/flume.conf

   - Start the log generator/ simulator
     1. open run_start_simulator.bat from file explorer
     OR
     2.
     ```
     cd api\src; python
     from simulation.simulator import NewsSimulator
     simulator = NewsSimulator()
     simulator.generate_continuous_logs()
     ```
Flume will:
-Monitor the logs directory
-Pick up new log files
-Send them to Kafka topic 'news_exposure_logs'

The ETL pipeline will:
-Consume messages from Kafka
-Process and enrich the logs
-Store them in PostgreSQL

You can verify the flow by:
-Checking Kafka topics: 
  ```
  docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
  docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic news_exposure_logs --max-messages 5
  ```
-Viewing PostgreSQL data: Connect to DB and query the tables
  ```
  docker-compose exec postgres psql -U newsuser -d newsdb
  ```
-Monitoring Flume metrics: Through the Flume metrics endpoint

## Maintenance
-Clean up docker
  ```
  docker-compose down --remove-orphans
  docker builder prune
  docker volume prune
  ```
## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.
