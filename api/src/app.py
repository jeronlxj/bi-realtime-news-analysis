from flask import Flask, jsonify, request
from etl.etl_pipeline import NewsETLPipeline
from analysis.analyzer import NewsAnalyzer
from storage.db import DatabaseConnection, News
from simulation.simulator import NewsSimulator
import threading
import time
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import psycopg2
import os

app = Flask(__name__)

def wait_for_services():
    """Wait for Kafka, PostgreSQL, and Spark to be ready"""
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
    
    # Wait for Spark Master
    spark_retries = 0
    while spark_retries < max_retries:
        try:
            import socket
            spark_master_host = os.getenv('SPARK_MASTER_URL', 'spark://spark:7077').replace('spark://', '').split(':')[0]
            spark_master_port = int(os.getenv('SPARK_MASTER_URL', 'spark://spark:7077').split(':')[-1])
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((spark_master_host, spark_master_port))
            sock.close()
            
            if result == 0:
                print("Successfully connected to Spark Master")
                break
            else:
                raise Exception("Connection failed")
                
        except Exception as e:
            print(f"Waiting for Spark Master to be ready... (attempt {spark_retries + 1}/{max_retries}): {str(e)}")
            spark_retries += 1
            time.sleep(3)
    
    if spark_retries >= max_retries:
        print("Warning: Could not connect to Spark Master after maximum retries. Spark functionality may be limited.")

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

# to run simulator in docker, the PENS dataset files must have been copied to a volume in the container
# def start_simulator():
#     """Run the news simulator in a loop"""
#     while True:
#         try:
#             print("Starting news simulator...")
#             simulator = NewsSimulator()
#             simulator.generate_continuous_logs()
#         except FileNotFoundError as e:
#             print(f"Error in simulator - File not found: {e}")
#             print("Please ensure the PENS dataset files are available")
#             raise  # Stop retrying if files are missing
#         except Exception as e:
#             print(f"Error in simulator: {str(e)}")
#             import traceback
#             print("Full traceback:")
#             print(traceback.format_exc())
#             time.sleep(10)  # Wait before retrying

# Wait for services to be ready
wait_for_services()

# Initialize database connection
db = DatabaseConnection()

# Initialize global variables for Spark components
analyzer = None
etl_pipeline = None

# # Start simulator in background thread - don't make it daemon so it won't be killed
# simulator_thread = threading.Thread(target=start_simulator, daemon=False)
# simulator_thread.start()

try:
    # Initialize Spark components with better error handling
    print("Initializing Spark components...")
    analyzer = NewsAnalyzer(kafka_bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'))
    print("Spark NewsAnalyzer initialized successfully")
    
    etl_pipeline = NewsETLPipeline()
    print("ETL pipeline initialized successfully")
    
    # Start ETL pipeline in background thread
    etl_thread = threading.Thread(target=start_etl_pipeline, daemon=True)
    etl_thread.start()
    print("ETL pipeline background thread started")
    
except Exception as e:
    print(f"Failed to initialize Spark components: {e}")
    print("Continuing without Spark functionality...")
    # Import traceback to show full error details
    import traceback
    print("Full error trace:")
    traceback.print_exc()

@app.route('/api/run_etl', methods=['POST'])
def run_etl():
    """Start the ETL pipeline"""
    global etl_pipeline
    try:
        if etl_pipeline is None:
            etl_pipeline = NewsETLPipeline()
        etl_pipeline.run_pipeline()
        return jsonify({"message": "ETL pipeline started successfully."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/analyze', methods=['GET'])
def analyze():
    """Analyze news data based on query parameters"""
    global analyzer
    try:
        if analyzer is None:
            return jsonify({"error": "Spark analyzer not available. Check Spark connection."}), 503
            
        query = request.args.get('query')
        if not query:
            return jsonify({"error": "Query parameter is required"}), 400
            
        # Start the analysis stream
        query_obj = analyzer.analyze_stream()
        return jsonify({
            "message": "Analysis started successfully",
            "query_id": query_obj.id if query_obj else "unknown"
        }), 200
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