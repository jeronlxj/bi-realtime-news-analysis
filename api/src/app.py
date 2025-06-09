from flask import Flask, jsonify
from etl.etl_pipeline import run_etl_pipeline
from analysis.analyzer import analyze_data
from storage.db import get_news_data

app = Flask(__name__)

@app.route('/api/run_etl', methods=['POST'])
def run_etl():
    run_etl_pipeline()
    return jsonify({"message": "ETL pipeline executed successfully."}), 200

@app.route('/api/analyze', methods=['GET'])
def analyze():
    query = request.args.get('query')
    results = analyze_data(query)
    return jsonify(results), 200

@app.route('/api/news', methods=['GET'])
def get_news():
    news_data = get_news_data()
    return jsonify(news_data), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)