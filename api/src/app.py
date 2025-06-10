from flask import Flask, jsonify, request
from etl.etl_pipeline import NewsETLPipeline
from analysis.analyzer import NewsAnalyzer
from storage.db import DatabaseConnection, News  # Added News model import

app = Flask(__name__)

# Initialize components
etl_pipeline = NewsETLPipeline()
analyzer = NewsAnalyzer(kafka_bootstrap_servers='localhost:9092')
db = DatabaseConnection()

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