# Real-Time News Dynamic Change Analysis System

This project implements a real-time dynamic change analysis system for news topics. It consists of several modules that work together to simulate, extract, transform, load (ETL), store, analyze, and visualize news data.

## Project Structure

- **api/**: Contains the backend application built with Flask.
  - **src/**: Source code for the backend.
    - **app.py**: Main entry point for the backend application.
    - **etl/**: Module for ETL processes.
      - **etl_pipeline.py**: Implementation of the ETL pipeline using the Flume framework.
    - **analysis/**: Module for data analysis.
      - **analyzer.py**: Provides analysis functionalities using Spark Streaming.
    - **storage/**: Module for database interactions.
      - **db.py**: Handles data storage and retrieval.
    - **simulation/**: Module for simulating news exposure logs.
      - **simulator.py**: Generates time series logs for news exposure and clickstream data.
  - **requirements.txt**: Lists the Python dependencies required for the backend application.

## Setup Instructions

1. Clone the repository:
   ```
   git clone <repository-url>
   cd realtime-news-bi-analysis/api
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Run the application:
   ```
   python src/app.py
   ```

## Usage

- The backend API provides endpoints for interacting with the news data. You can use tools like Postman or cURL to make requests to the API.
- The ETL pipeline collects news exposure logs and sends them to the Kafka queue for processing.
- The analysis module uses Spark Streaming to analyze the data and generate insights based on user queries.

## Module Descriptions

- **ETL Pipeline**: Collects and processes news data, sending it to Kafka for further analysis.
- **Analyzer**: Analyzes the news data in real-time and provides insights based on user-defined queries.
- **Database**: Manages data storage and retrieval, ensuring efficient access to the news data.
- **Simulator**: Simulates news exposure logs for testing and development purposes.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.