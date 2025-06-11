from flask import Flask, jsonify, request
from etl.etl_pipeline import NewsETLPipeline
from analysis.analyzer import NewsAnalyzer
from storage.db import DatabaseConnection, News
import threading
import time
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import psycopg2
import os

app = Flask(__name__)

def wait_for_services():
    """Wait for Kafka and PostgreSQL to be ready"""
    # Wait for Kafka
    kafka_retries = 0
    max_retries = 30
    while kafka_retries < max_retries:
        try:
            kafka_host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
            admin_client = KafkaAdminClient(
                bootstrap_servers=kafka_host,
                request_timeout_ms=10000,
                connections_max_idle_ms=60000
            )
            admin_client.close()
            print("Successfully connected to Kafka")
            break
        except Exception as e:
            print(f"Waiting for Kafka to be ready... (attempt {kafka_retries + 1}/{max_retries}): {str(e)}")
            kafka_retries += 1
            time.sleep(5)
    
    if kafka_retries >= max_retries:
        print("Warning: Could not connect to Kafka after maximum retries. Continuing anyway...")
    
    # Wait for PostgreSQL
    postgres_retries = 0
    while postgres_retries < max_retries:
        try:
            db_host = os.getenv('POSTGRES_HOST', 'postgres')
            conn = psycopg2.connect(
                dbname="newsdb",
                user="newsuser",
                password="newspass",
                host=db_host
            )
            conn.close()
            print("Successfully connected to PostgreSQL")
            break
        except psycopg2.OperationalError as e:
            print(f"Waiting for PostgreSQL to be ready... (attempt {postgres_retries + 1}/{max_retries}): {str(e)}")
            postgres_retries += 1
            time.sleep(3)

def start_etl_pipeline():
    """Run the ETL pipeline in a loop"""
    while True:
        try:
            print("Starting ETL pipeline...")
            etl_pipeline = NewsETLPipeline()
            etl_pipeline.run_pipeline()
        except Exception as e:
            print(f"Error in ETL pipeline: {e}")
            time.sleep(10)  # Wait before retrying

# Wait for services to be ready
wait_for_services()

# Initialize components
etl_pipeline = NewsETLPipeline()
analyzer = NewsAnalyzer(kafka_bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'))
db = DatabaseConnection()

# Start ETL pipeline in background thread
etl_thread = threading.Thread(target=start_etl_pipeline, daemon=True)
etl_thread.start()

@app.route('/api/run_etl', methods=['POST'])
def run_etl():
    """Start the ETL pipeline"""
    try:
        etl_pipeline.run_pipeline()
        return jsonify({"message": "ETL pipeline started successfully."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/analyze', methods=['GET'])
def analyze():
    """Analyze news data based on query parameters"""
    try:
        query = request.args.get('query')
        if not query:
            return jsonify({"error": "Query parameter is required"}), 400
            
        analyzer.analyze_stream()  # Start the analysis
        return jsonify({"message": "Analysis started successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/news', methods=['GET'])
def get_news():
    """Get news data from the database"""
    try:
        with db.session.begin():
            news_data = db.session.query(News).all()
            # Convert to dictionary format
            result = [{
                'news_id': news.news_id,
                'category': news.category,
                'topic': news.topic,
                'headline': news.headline,
                'news_body': news.news_body
            } for news in news_data]
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)