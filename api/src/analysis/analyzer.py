from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
import os

class NewsAnalyzer:
    def __init__(self, kafka_bootstrap_servers):
        # Connect to the external Spark cluster
        spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark:7077')
        
        self.spark = SparkSession.builder \
            .appName("NewsAnalyzer") \
            .master(spark_master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
            
        self.kafka_bootstrap_servers = kafka_bootstrap_servers

    def analyze_stream(self):
        news_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "news_topic") \
            .load()

        # Assuming the value is in JSON format
        news_data = news_stream.selectExpr("CAST(value AS STRING)")

        # Perform analysis (e.g., count news articles per topic)
        analysis_result = news_data.groupBy(
            window(col("timestamp"), "10 minutes"),
            col("topic")
        ).count()

        query = analysis_result.writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()

        query.awaitTermination()

    def stop(self):
        self.spark.stop()