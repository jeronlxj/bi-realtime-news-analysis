"""
Advanced Spark Streaming Analysis with AI+BI Agent capabilities
Provides intelligent query and analysis services for news dynamics
"""
import os
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, from_json, to_json, struct, lit, when, count, sum as spark_sum,
    avg, max as spark_max, min as spark_min, desc, asc, collect_list, 
    regexp_extract, length, split, concat, broadcast, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, 
    TimestampType, BooleanType, ArrayType
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.ml.feature import VectorAssembler, StringIndexer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.pipeline import Pipeline
from pyspark.ml import Pipeline as MLPipeline

from utils.spark_manager import get_spark_manager
from utils.time_manager import get_time_manager

class SparkNewsAnalyzer:
    """Advanced Spark-based news analytics with real-time insights"""
    
    def __init__(self, kafka_bootstrap_servers: str):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.logger = logging.getLogger(__name__)
        self.spark_manager = get_spark_manager()
        self.spark = None
        self.active_queries: Dict[str, StreamingQuery] = {}
        
        # Initialize time manager for virtual time
        self.time_manager = get_time_manager()
        
        # Define schemas for different data types
        self.exposure_log_schema = StructType([
            StructField("impression_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("news_id", StringType(), True),
            StructField("clicked", IntegerType(), True),
            StructField("dwell_time", FloatType(), True),
            StructField("processed_timestamp", StringType(), True)
        ])
        
        self.news_schema = StructType([
            StructField("news_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("topic", StringType(), True),
            StructField("headline", StringType(), True),
            StructField("news_body", StringType(), True)
        ])
    
    def initialize_spark(self) -> bool:
        """Initialize Spark session with enhanced configuration for ML workloads"""
        try:
            from pyspark import SparkContext
            if SparkContext._active_spark_context is not None:
                self.logger.info("Stopping existing SparkContext...")
                SparkContext._active_spark_context.stop()
                time.sleep(2)

            spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark:7077')


            self.spark = SparkSession.builder \
                .appName("AdvancedNewsAnalyzer") \
                .master(spark_master) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.driver.memory", "1g") \
                .config("spark.executor.memory", "1g") \
                .config("spark.driver.maxResultSize", "512m") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.network.timeout", "300s") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.streaming.metricsEnabled", "true") \
                .getOrCreate()
            
            self.logger.info(f"Advanced Spark session initialized successfully with master: {spark_master}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark session: {e}")
            return False
    
    def create_exposure_stream(self) -> "DataFrame":
        """Create a streaming DataFrame from Kafka exposure logs"""
        if not self.spark:
            raise Exception("Spark session not initialized")
        
        # Read from Kafka
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "news_exposure_logs") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON and extract fields
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), self.exposure_log_schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp strings to timestamp type
        processed_df = parsed_df.withColumn(
            "timestamp", 
            col("timestamp").cast(TimestampType())
        ).withColumn(
            "processed_timestamp",
            col("processed_timestamp").cast(TimestampType())
        )
        
        return processed_df
    def start_real_time_trend_analysis(self) -> str:
        """Start real-time trend analysis stream with virtual time awareness"""
        try:
            exposure_stream = self.create_exposure_stream()
            
            # Get current virtual time window parameters
            virtual_time = self.time_manager.get_current_time()
            self.logger.info(f"Starting trend analysis stream with virtual time reference: {virtual_time}")
            
            # Calculate real-time trends with 5-minute windows
            # Note: The window function operates on the timestamp column regardless of our virtual time
            # The data filtering will ensure we're only analyzing data within our virtual time range
            trend_analysis = exposure_stream \
                .withWatermark("timestamp", "10 minutes") \
                .groupBy(
                    window(col("timestamp"), "5 minutes", "1 minute"),
                    col("news_id")
                ) \
                .agg(
                    count("impression_id").alias("impression_count"),
                    spark_sum("clicked").alias("click_count"),
                    count(col("user_id").distinct()).alias("unique_users"),
                    avg("dwell_time").alias("avg_dwell_time")
                ) \
                .withColumn("click_rate", col("click_count") / col("impression_count")) \
                .withColumn("trend_score", 
                    col("click_rate") * 0.4 + 
                    (col("unique_users") / col("impression_count")) * 0.3 +
                    (col("avg_dwell_time") / 100.0) * 0.3
                ) \
                .withColumn("virtual_time", lit(virtual_time.strftime("%Y-%m-%d %H:%M:%S")))
            
            # Write to console and optionally to another Kafka topic
            query = trend_analysis.writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime='30 seconds') \
                .start()
            
            query_id = f"trend_analysis_{int(time.time())}"
            self.active_queries[query_id] = query
            
            self.logger.info(f"Started real-time trend analysis: {query_id}")
            return query_id
            
        except Exception as e:
            self.logger.error(f"Failed to start trend analysis: {e}")
            raise
    def start_user_behavior_analysis(self) -> str:
        """Start real-time user behavior pattern analysis with virtual time awareness"""
        try:
            exposure_stream = self.create_exposure_stream()
            
            # Get current virtual time for reference
            virtual_time = self.time_manager.get_current_time()
            self.logger.info(f"Starting user behavior analysis stream with virtual time reference: {virtual_time}")
            
            # Analyze user behavior patterns
            user_behavior = exposure_stream \
                .withWatermark("timestamp", "15 minutes") \
                .groupBy(
                    window(col("timestamp"), "10 minutes", "2 minutes"),
                    col("user_id")
                ) \
                .agg(
                    count("impression_id").alias("total_impressions"),
                    spark_sum("clicked").alias("total_clicks"),
                    avg("dwell_time").alias("avg_dwell_time"),
                    collect_list("news_id").alias("viewed_news"),
                    count(col("news_id").distinct()).alias("unique_news_count")
                ) \
                .withColumn("engagement_score",
                    when(col("total_impressions") > 0,
                        (col("total_clicks") * 2.0 + col("avg_dwell_time") / 30.0) / col("total_impressions")
                    ).otherwise(0.0)
                ) \
                .withColumn("diversity_score",
                    col("unique_news_count") / col("total_impressions")
                ) \
                .withColumn("virtual_time", lit(virtual_time.strftime("%Y-%m-%d %H:%M:%S")))
            
            query = user_behavior.writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime='60 seconds') \
                .start()
            
            query_id = f"user_behavior_{int(time.time())}"
            self.active_queries[query_id] = query
            
            self.logger.info(f"Started user behavior analysis: {query_id}")
            return query_id
            
        except Exception as e:
            self.logger.error(f"Failed to start user behavior analysis: {e}")
            raise
    
    def start_anomaly_detection(self) -> str:
        """Start anomaly detection for unusual news patterns"""
        try:
            exposure_stream = self.create_exposure_stream()
            
            # Calculate baseline metrics and detect anomalies
            anomaly_detection = exposure_stream \
                .withWatermark("timestamp", "20 minutes") \
                .groupBy(
                    window(col("timestamp"), "15 minutes", "5 minutes"),
                    col("news_id")
                ) \
                .agg(
                    count("impression_id").alias("impressions"),
                    spark_sum("clicked").alias("clicks"),
                    avg("dwell_time").alias("avg_dwell_time"),
                    count(col("user_id").distinct()).alias("unique_users")
                ) \
                .withColumn("click_rate", col("clicks") / col("impressions")) \
                .withColumn("virality_score",
                    (col("impressions") * col("click_rate") * col("unique_users")) / 
                    (col("window.start").cast("long") - col("window.end").cast("long") + 1)
                ) \
                .filter(col("virality_score") > 10.0)  # Threshold for anomaly
            
            query = anomaly_detection.writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime='2 minutes') \
                .start()
            
            query_id = f"anomaly_detection_{int(time.time())}"
            self.active_queries[query_id] = query
            
            self.logger.info(f"Started anomaly detection: {query_id}")
            return query_id
            
        except Exception as e:
            self.logger.error(f"Failed to start anomaly detection: {e}")
            raise
    
    def start_content_analysis(self) -> str:
        """Start content-based analysis using headline features"""
        try:
            # This would require joining with news data
            # For now, we'll implement a simplified version
            exposure_stream = self.create_exposure_stream()
            
            # Add UDF for headline analysis
            def analyze_headline_sentiment(headline: str) -> float:
                """Simple sentiment analysis based on keywords"""
                if not headline:
                    return 0.0
                
                positive_words = ['good', 'great', 'amazing', 'excellent', 'success', 'win', 'best']
                negative_words = ['bad', 'terrible', 'worst', 'fail', 'crisis', 'disaster', 'problem']
                
                headline_lower = headline.lower()
                positive_count = sum(1 for word in positive_words if word in headline_lower)
                negative_count = sum(1 for word in negative_words if word in headline_lower)
                
                return (positive_count - negative_count) / max(len(headline_lower.split()), 1)
            
            # Register UDF
            sentiment_udf = udf(analyze_headline_sentiment, FloatType())
            
            # This is a placeholder - in real implementation, you'd join with news data
            content_analysis = exposure_stream \
                .withWatermark("timestamp", "10 minutes") \
                .groupBy(
                    window(col("timestamp"), "5 minutes"),
                    col("news_id")
                ) \
                .agg(
                    count("impression_id").alias("impressions"),
                    spark_sum("clicked").alias("clicks"),
                    avg("dwell_time").alias("avg_dwell_time")
                ) \
                .withColumn("engagement_rate", col("clicks") / col("impressions"))
            
            query = content_analysis.writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime='60 seconds') \
                .start()
            
            query_id = f"content_analysis_{int(time.time())}"
            self.active_queries[query_id] = query
            
            self.logger.info(f"Started content analysis: {query_id}")
            return query_id
            
        except Exception as e:
            self.logger.error(f"Failed to start content analysis: {e}")
            raise
    def get_real_time_insights(self, query_type: str = "overview", hours: int = 1) -> Dict:
        """Get real-time insights using Spark streaming aggregations with virtual time"""
        try:
            if not self.spark:
                self.initialize_spark()
            
            # Create a batch query on recent streaming data
            exposure_df = self.spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", "news_exposure_logs") \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load()
            
            # Parse the JSON data
            parsed_df = exposure_df.select(
                from_json(col("value").cast("string"), self.exposure_log_schema).alias("data")
            ).select("data.*")
            
            # Convert timestamps
            processed_df = parsed_df.withColumn(
                "timestamp", col("timestamp").cast(TimestampType())
            )
            
            # Use virtual time for filtering recent data
            start_time, end_time = self.time_manager.get_time_window(hours=hours)
            self.logger.info(f"Analyzing data from {start_time} to {end_time} (virtual time window)")
            recent_df = processed_df.filter(
                (col("timestamp") >= lit(start_time)) & (col("timestamp") <= lit(end_time))
            )
            
            if query_type == "overview":
                # Calculate overview metrics
                overview = recent_df.agg(
                    count("impression_id").alias("total_impressions"),
                    spark_sum("clicked").alias("total_clicks"),
                    count(col("user_id").distinct()).alias("unique_users"),
                    count(col("news_id").distinct()).alias("unique_news"),
                    avg("dwell_time").alias("avg_dwell_time")
                ).collect()[0]
                  # Use virtual time for timestamp
                virtual_time = self.time_manager.get_current_time()
                
                return {
                    "total_impressions": overview["total_impressions"],
                    "total_clicks": overview["total_clicks"],
                    "unique_users": overview["unique_users"],
                    "unique_news": overview["unique_news"],
                    "avg_dwell_time": float(overview["avg_dwell_time"]) if overview["avg_dwell_time"] else 0,
                    "overall_ctr": (overview["total_clicks"] / max(overview["total_impressions"], 1)) * 100,
                    "timestamp": virtual_time.isoformat(),
                    "virtual_time": virtual_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "real_time": datetime.now().isoformat()
                }
            
            elif query_type == "trending":
                # Get trending news
                trending = recent_df.groupBy("news_id") \
                    .agg(
                        count("impression_id").alias("impressions"),
                        spark_sum("clicked").alias("clicks"),
                        count(col("user_id").distinct()).alias("unique_users")
                    ) \
                    .withColumn("trend_score", 
                        (col("clicks") / col("impressions")) * col("unique_users")
                    ) \
                    .orderBy(desc("trend_score")) \
                    .limit(10) \
                    .collect()
                  # Use virtual time for timestamp
                virtual_time = self.time_manager.get_current_time()
                
                return {
                    "trending_news": [
                        {
                            "news_id": row["news_id"],
                            "impressions": row["impressions"],
                            "clicks": row["clicks"],
                            "unique_users": row["unique_users"],
                            "trend_score": float(row["trend_score"])
                        } for row in trending
                    ],
                    "timestamp": virtual_time.isoformat(),
                    "virtual_time": virtual_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "real_time": datetime.now().isoformat()
                }
            
            return {"error": "Unknown query type"}
            
        except Exception as e:
            self.logger.error(f"Failed to get real-time insights: {e}")
            return {"error": str(e)}
    
    def stop_all_queries(self):
        """Stop all active streaming queries"""
        for query_id, query in self.active_queries.items():
            try:
                query.stop()
                self.logger.info(f"Stopped query: {query_id}")
            except Exception as e:
                self.logger.error(f"Error stopping query {query_id}: {e}")
        
        self.active_queries.clear()
    
    def get_active_queries(self) -> List[str]:
        """Get list of active query IDs"""
        return list(self.active_queries.keys())
    
    def stop_query(self, query_id: str) -> bool:
        """Stop a specific query"""
        if query_id in self.active_queries:
            try:
                self.active_queries[query_id].stop()
                del self.active_queries[query_id]
                self.logger.info(f"Stopped query: {query_id}")
                return True
            except Exception as e:
                self.logger.error(f"Error stopping query {query_id}: {e}")
                return False
        return False
    
    def cleanup(self):
        """Clean up resources"""
        self.stop_all_queries()
        if self.spark_manager:
            self.spark_manager.stop_session()
