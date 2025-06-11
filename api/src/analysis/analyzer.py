import os
import time
import logging

class NewsAnalyzer:
    def __init__(self, kafka_bootstrap_servers):
        self.logger = logging.getLogger(__name__)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.spark = None
        
        # Delay Spark initialization to avoid early failures
        self._spark_initialized = False
        
    def _initialize_spark_session(self):
        """Initialize Spark session with proper error handling"""
        if self._spark_initialized:
            return
            
        try:
            from pyspark.sql import SparkSession
            from pyspark import SparkContext
            
            spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark:7077')
            
            # Stop any existing SparkContext
            if SparkContext._active_spark_context is not None:
                self.logger.info("Stopping existing SparkContext...")
                SparkContext._active_spark_context.stop()
                time.sleep(3)
            
            # Create SparkSession with proper configuration
            self.spark = SparkSession.builder \
                .appName("NewsAnalyzer") \
                .master(spark_master) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.driver.memory", "1g") \
                .config("spark.executor.memory", "1g") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.network.timeout", "300s") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            self.logger.info(f"Successfully initialized Spark session with master: {spark_master}")
            self._spark_initialized = True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark session: {str(e)}")
            raise

    def analyze_stream(self):
        """Analyze streaming data from Kafka"""
        if not self._spark_initialized:
            self._initialize_spark_session()
        
        if not self.spark:
            raise Exception("Spark session not available")
        
        try:
            from pyspark.sql.functions import col, window, from_json
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
            
            # Read from Kafka stream
            news_stream = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", "news_exposure_logs") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()

            # Define schema for the JSON data
            schema = StructType([
                StructField("impression_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("news_id", StringType(), True),
                StructField("clicked", IntegerType(), True),
                StructField("dwell_time", FloatType(), True),
                StructField("processed_timestamp", StringType(), True)
            ])
            
            # Parse JSON data
            parsed_stream = news_stream.select(
                from_json(col("value").cast("string"), schema).alias("data")
            ).select("data.*")

            # Perform analysis (count clicks per news article)
            analysis_result = parsed_stream \
                .filter(col("clicked") == 1) \
                .groupBy(
                    window(col("timestamp").cast("timestamp"), "10 minutes"),
                    col("news_id")
                ) \
                .count()

            # Write results to console
            query = analysis_result.writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime='30 seconds') \
                .start()

            self.logger.info(f"Started streaming query with ID: {query.id}")
            return query

        except Exception as e:
            self.logger.error(f"Error in stream analysis: {str(e)}")
            raise

    def stop(self):
        """Safely stop the Spark session"""
        if self.spark:
            try:
                self.logger.info("Stopping Spark session...")
                self.spark.stop()
                self.spark = None
                self._spark_initialized = False
                self.logger.info("Spark session stopped successfully")
            except Exception as e:
                self.logger.error(f"Error stopping Spark session: {str(e)}")
    
    def __del__(self):
        """Destructor to ensure Spark session is properly closed"""
        self.stop()