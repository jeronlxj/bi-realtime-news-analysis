from flask import Flask, jsonify, request
from flask_cors import CORS
from etl.etl_pipeline import NewsETLPipeline
from analysis.spark_analyzer import SparkNewsAnalyzer
from storage.db import DatabaseConnection, News
from simulation.simulator import NewsSimulator
from utils.time_manager import get_time_manager
import threading
import time
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import psycopg2
import os
import pytz
import traceback
import socket
import google.generativeai as genai
from datetime import datetime, timedelta
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend communication

# Initialize Gemini API
def initialize_gemini():
    api_key = os.getenv('GEMINI_API_KEY', '')
    if api_key:
        genai.configure(api_key=api_key)
        print("Gemini API initialized successfully")
        return True
    else:
        print("Warning: Gemini API key not found. AI/BI agent will not work properly.")
        return False

# Initialize Gemini when the app starts
gemini_initialized = initialize_gemini()

def get_dataset_date_range(start_date_str=None, end_date_str=None):
    """Get default date range based on the PENS dataset if not provided"""
    # Use the global time_manager instance
    global time_manager
    current_virtual_time = time_manager.get_current_time()
    
    # Set default date ranges to match dataset period if not provided
    if not start_date_str:
        start_date = datetime(2019, 6, 14)  # Dataset starts from June 14, 2019
    else:
        start_date = datetime.fromisoformat(start_date_str)
        
    if not end_date_str:
        # Use the current virtual time as the end date instead of a fixed date
        # This ensures we're using the most current data available in our virtual timeline
        end_date = current_virtual_time
        
        # Make sure we don't exceed the dataset's actual end date
        max_dataset_date = datetime(2019, 7, 5)  # Dataset ends around July 5, 2019
        if end_date > max_dataset_date:
            end_date = max_dataset_date
            print(f"Warning: Virtual time {current_virtual_time} exceeds dataset end date. Using {max_dataset_date} instead.")
    else:
        end_date = datetime.fromisoformat(end_date_str)
    
    print(f"Using date range: {start_date} to {end_date} (virtual now: {current_virtual_time})")
    return start_date, end_date

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

# Initialize the time manager for virtual time - we should see only one initialization
# as the TimeManager is a singleton
time_manager = get_time_manager()
print(f"Virtual time initialized to: {time_manager.get_current_time().strftime('%Y-%m-%d %H:%M:%S')}")

# Initialize database connection
db = DatabaseConnection()

# Initialize global variables for Spark components
analyzer = None
spark_analyzer = None
etl_pipeline = None

# # Start simulator in background thread - don't make it daemon so it won't be killed
# simulator_thread = threading.Thread(target=start_simulator, daemon=False)
# simulator_thread.start()

try:    # Initialize Spark components with better error handling
    print("Initializing Spark components...")
    
    # Initialize advanced Spark analyzer
    spark_analyzer = SparkNewsAnalyzer(kafka_bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'))
    if spark_analyzer.initialize_spark():
        print("Advanced Spark analyzer initialized successfully")
    else:
        print("Warning: Advanced Spark analyzer initialization failed")
        spark_analyzer = None    
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
        # Add pagination parameters with defaults
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        category = request.args.get('category')
        
        # Basic logging for debugging
        print(f"Fetching news with limit={limit}, offset={offset}, category={category}")
        
        try:
            query = db.session.query(News)
            
            # Add category filter if provided
            if category:
                query = query.filter(News.category == category)
            
            # Add pagination
            news_data = query.limit(limit).offset(offset).all()
            
            # Convert to dictionary format
            result = [{
                'news_id': news.news_id,
                'category': news.category,
                'topic': news.topic,
                'headline': news.headline,
                'news_body': news.news_body,
                'news_body_preview': (news.news_body[:150] + '...') if news.news_body and len(news.news_body) > 150 else news.news_body
            } for news in news_data]
            
            return jsonify({
                'data': result,
                'count': len(result),
                'offset': offset,
                'has_more': len(result) == limit
            }), 200
        except Exception as e:
            print(f"Database error: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        print(f"General error in get_news: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

# New high-performance analytics endpoints

@app.route('/api/analytics/news-lifecycle/<news_id>', methods=['GET'])
def get_news_lifecycle(news_id):
    """Get lifecycle analysis for a specific news article"""
    try:
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        
        # Use utility function to get appropriate date range
        start_date, end_date = get_dataset_date_range(start_date_str, end_date_str)
        
        result = db.get_news_lifecycle(news_id, start_date, end_date)
        return jsonify({
            "news_id": news_id,
            "lifecycle_data": result,
            "query_timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/category-trends', methods=['GET'])
def get_category_trends():
    """Get category trend analysis"""
    try:
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        
        # Use utility function to get appropriate date range
        start_date, end_date = get_dataset_date_range(start_date_str, end_date_str)
        
        result = db.get_category_trends(start_date, end_date)
        return jsonify({
            "trends": result,
            "query_timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/user-interests', methods=['GET'])
def get_user_interests():
    """Get user interest analysis"""
    try:
        user_id = request.args.get('user_id')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        
        # Use utility function to get appropriate date range
        start_date, end_date = get_dataset_date_range(start_date_str, end_date_str)
        
        result = db.get_user_interest_changes(user_id, start_date, end_date)
        return jsonify({
            "user_interests": result,
            "query_timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/hot-news', methods=['GET'])
def get_hot_news():
    """Get hot news prediction analysis"""
    try:
        hours_ahead = int(request.args.get('hours_ahead', 24))
        min_impressions = int(request.args.get('min_impressions', 50))
        
        # We need to pass a reference date within the dataset period
        reference_date = time_manager.get_current_time()
        
        result = db.get_hot_news_prediction(hours_ahead, min_impressions, reference_date)
        return jsonify({
            "hot_news": result,
            "prediction_parameters": {
                "hours_ahead": hours_ahead,
                "min_impressions": min_impressions,
                "reference_date": reference_date.isoformat()
            },
            "query_timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/recommendations/<user_id>', methods=['GET'])
def get_recommendations(user_id):
    """Get personalized news recommendations for a user"""
    try:
        limit = int(request.args.get('limit', 10))
        
        # Use dataset date for reference
        reference_date = datetime(2019, 7, 1)
        
        result = db.get_user_recommendations(user_id, limit, reference_date)
        return jsonify({
            "user_id": user_id,
            "recommendations": result,
            "reference_date": reference_date.isoformat(),
            "query_timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/performance', methods=['GET'])
def get_performance_stats():
    """Get query performance statistics"""
    try:
        hours = int(request.args.get('hours', 24))
        
        # Use current date as reference since query logs are from current system
        # No need to use historical dates since performance metrics are about API usage
        
        result = db.get_query_performance_stats(hours)
        return jsonify({
            "performance_stats": result,
            "time_window_hours": hours,
            "query_timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/overview', methods=['GET'])
def get_analytics_overview():
    """Get a comprehensive analytics overview for dashboard"""
    try:
        # Use utility function to get appropriate date range
        start_date, end_date = get_dataset_date_range()
        
        # Gather multiple analytics in parallel
        category_trends = db.get_category_trends(start_date, end_date)
        hot_news = db.get_hot_news_prediction(24, 50, time_manager.get_current_time())
        performance_stats = db.get_query_performance_stats(24)
        
        # Calculate summary statistics
        total_categories = len(set(trend['category'] for trend in category_trends))
        total_impressions = sum(trend['impressions'] for trend in category_trends)
        total_clicks = sum(trend['clicks'] for trend in category_trends)
        overall_ctr = total_clicks / max(total_impressions, 1)
        
        return jsonify({
            "overview": {
                "time_period": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                },
                "summary": {
                    "total_categories": total_categories,
                    "total_impressions": total_impressions,
                    "total_clicks": total_clicks,
                    "overall_ctr": overall_ctr,
                    "trending_news_count": len(hot_news)
                }
            },
            "category_trends": category_trends[:50],  # Top 10 category_days
            "hot_news": hot_news[:5],  # Top 5 trending
            "performance": performance_stats,
            "query_timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat(),
            "virtual_timestamp": time_manager.get_current_time().isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Advanced Spark Streaming Analytics Endpoints

@app.route('/api/spark/start-trend-analysis', methods=['POST'])
def start_trend_analysis():
    """Start real-time trend analysis using Spark Streaming"""
    global spark_analyzer
    try:
        query_id = spark_analyzer.start_real_time_trend_analysis()
        return jsonify({
            "message": "Real-time trend analysis started",
            "query_id": query_id,
            "timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/spark/start-user-behavior', methods=['POST'])
def start_user_behavior_analysis():
    """Start real-time user behavior analysis using Spark Streaming"""
    global spark_analyzer
    try:
        query_id = spark_analyzer.start_user_behavior_analysis()
        return jsonify({
            "message": "User behavior analysis started",
            "query_id": query_id,
            "timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/spark/start-anomaly-detection', methods=['POST'])
def start_anomaly_detection():
    """Start anomaly detection using Spark Streaming"""
    global spark_analyzer
    try:
        query_id = spark_analyzer.start_anomaly_detection()
        return jsonify({
            "message": "Anomaly detection started",
            "query_id": query_id,
            "timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/spark/real-time-insights', methods=['GET'])
def get_real_time_insights():
    """Get real-time insights from Spark streaming data"""
    global spark_analyzer
    try:
        if spark_analyzer is None:
            return jsonify({"error": "Advanced Spark analyzer not available"}), 503
        
        query_type = request.args.get('type', 'overview')
        insights = spark_analyzer.get_real_time_insights(query_type)
        
        return jsonify({
            "insights": insights,
            "query_type": query_type,
            "timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/spark/queries', methods=['GET'])
def get_active_spark_queries():
    """Get list of active Spark streaming queries"""
    global spark_analyzer
    try:
        if spark_analyzer is None:
            return jsonify({"error": "Advanced Spark analyzer not available"}), 503
        
        active_queries = spark_analyzer.get_active_queries()
        return jsonify({
            "active_queries": active_queries,
            "total_count": len(active_queries),
            "timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/spark/queries/<query_id>', methods=['DELETE'])
def stop_spark_query(query_id):
    """Stop a specific Spark streaming query"""
    global spark_analyzer
    try:
        if spark_analyzer is None:
            return jsonify({"error": "Advanced Spark analyzer not available"}), 503
        
        success = spark_analyzer.stop_query(query_id)
        if success:
            return jsonify({
                "message": f"Query {query_id} stopped successfully",
                "timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
            }), 200
        else:
            return jsonify({
                "error": f"Query {query_id} not found or failed to stop"
            }), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/spark/stop-all', methods=['POST'])
def stop_all_spark_queries():
    """Stop all active Spark streaming queries"""
    global spark_analyzer
    try:
        if spark_analyzer is None:
            return jsonify({"error": "Advanced Spark analyzer not available"}), 503
        
        spark_analyzer.stop_all_queries()
        return jsonify({
            "message": "All Spark queries stopped successfully",
            "timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/time/current', methods=['GET'])
def get_current_time():
    """Return the current virtual time"""
    current_virtual_time = time_manager.get_current_time()
    
    return jsonify({
        "virtual_time": current_virtual_time.strftime("%Y-%m-%d %H:%M:%S"),
        "virtual_timestamp": current_virtual_time.isoformat(),
        "real_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "real_timestamp": datetime.now().replace(tzinfo=pytz.UTC).isoformat()
    }), 200

# AI/BI Agent Endpoint

@app.route('/api/ai-query', methods=['POST'])
def ai_query_v2():
    """Process natural language queries using Gemini API and return database results"""
    try:
        if not gemini_initialized:
            return jsonify({
                'success': False,
                'error': 'Gemini API not initialized. Check API key configuration.'
            }), 500
        
        # Get the natural language query from the request
        data = request.json
        if not data or 'query' not in data:
            return jsonify({
                'success': False,
                'error': 'No query provided'
            }), 400
        
        nlquery = data['query']
        
        # Database schema information to provide context to the model
        schema_info = """
        Database tables:
        1. exposure_logs - Contains information about user interactions with news articles
           - impression_id: unique identifier for the exposure event
           - user_id: identifier for the user
           - news_id: identifier for the news article
           - timestamp: when the exposure occurred
           - clicked: whether the user clicked the article (1) or not (0)
           - dwell_time: how long the user spent on the article (seconds)
           - processed_timestamp: when this log was processed

        2. news - Contains information about news articles
           - news_id: unique identifier for the article
           - category: article category (e.g., politics, sports)
           - topic: specific topic within the category
           - headline: article headline
           - news_body: full article text
           - title_entity: JSON containing entities mentioned in the title
           - entity_content: JSON containing entities mentioned in the content

        Note: The data spans from June 14, 2019 to July 5, 2019.
        Current virtual time is: {current_time}
        """
        
        # Get current virtual time
        current_time = time_manager.get_current_time().strftime("%Y-%m-%d %H:%M:%S")
        schema_info = schema_info.format(current_time=current_time)
        
        # Prepare prompt for Gemini
        prompt = f"""
        You are a data analyst assistant for a news analytics system. 
        Convert the following natural language query into a valid SQL query for PostgreSQL.
        ONLY return the raw SQL query without any markdown formatting, quotes, or explanation.
        Do not include ```sql or ``` markers in your response.
        
        {schema_info}
        
        User query: "{nlquery}"
        
        SQL query:
        """
        
        # Generate SQL with Gemini
        sql_query = ""
        if gemini_initialized:
            try:
                model = genai.GenerativeModel('gemini-2.0-flash')
                response = model.generate_content(prompt)
                sql_query = response.text.strip()
                # Remove any potential markdown code block syntax
                if sql_query.startswith("```"):
                    sql_query = sql_query.split("```")[1]
                    if sql_query.startswith("sql"):
                        sql_query = sql_query[3:].strip()
                if sql_query.endswith("```"):
                    sql_query = sql_query[:-3].strip()
                print(f"Generated SQL query: {sql_query}")
            except Exception as e:
                print(f"Error generating SQL with Gemini: {e}")
                return jsonify({
                    'success': False,
                    'error': f'Error generating SQL query: {str(e)}'
                }), 500
        else:
            # Fallback if Gemini is not initialized
            # Simple mapping for common queries (very limited)
            if "most popular news" in nlquery.lower():
                sql_query = """
                SELECT n.headline, n.category, COUNT(e.news_id) as view_count 
                FROM news n JOIN exposure_logs e ON n.news_id = e.news_id 
                GROUP BY n.news_id, n.headline, n.category 
                ORDER BY view_count DESC LIMIT 10
                """
            else:
                return jsonify({
                    'success': False,
                    'error': 'Cannot process query without Gemini API. Please configure API key.'
                }), 500
        
        # Execute the SQL query
        try:
            # Connect to the database
            db_host = os.getenv('POSTGRES_HOST', 'postgres')
            conn = psycopg2.connect(
                dbname="newsdb",
                user="newsuser", 
                password="newspass",
                host=db_host
            )
            cursor = conn.cursor()
            
            # Execute the query with a timeout
            cursor.execute("SET statement_timeout = '60s'")
            cursor.execute(sql_query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch results
            results = cursor.fetchall()
            
            # Convert results to list of dictionaries
            formatted_results = []
            for row in results:
                row_dict = {}
                for i, col in enumerate(columns):
                    # Handle JSON fields
                    if isinstance(row[i], dict) or (isinstance(row[i], str) and (row[i].startswith('{') or row[i].startswith('['))):
                        try:
                            row_dict[col] = json.loads(row[i]) if isinstance(row[i], str) else row[i]
                        except json.JSONDecodeError:
                            row_dict[col] = row[i]
                    else:
                        # Handle datetime objects
                        if isinstance(row[i], datetime):
                            row_dict[col] = row[i].isoformat()
                        else:
                            row_dict[col] = row[i]
                formatted_results.append(row_dict)
            
            cursor.close()
            conn.close()
            
            return jsonify({
                'success': True,
                'query': nlquery,
                'sql': sql_query,
                'columns': columns,
                'results': formatted_results[:10]
            })
            
        except Exception as e:
            print(f"Error executing SQL query: {e}")
            # Log the failed query
            error_msg = str(e)
            
            return jsonify({
                'success': False,
                'query': nlquery,
                'sql': sql_query,
                'error': f'Error executing query: {error_msg}'
            }), 500
            
    except Exception as e:
        print(f"Unexpected error in AI query endpoint: {e}")
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)