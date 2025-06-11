"""
Simple Spark connection test to diagnose the SparkContext issue
"""
import os
import time
import sys

def test_spark_connection():
    """Test basic Spark connectivity"""
    
    print("Testing Spark connection...")
    print(f"Python version: {sys.version}")
    
    try:
        # Import PySpark
        print("Importing PySpark...")
        from pyspark.sql import SparkSession
        from pyspark import SparkContext, SparkConf
        print("✓ PySpark imported successfully")
        
        # Check environment variables
        spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark:7077')
        print(f"Spark Master URL: {spark_master}")
        
        # Test socket connection to Spark master
        print("Testing network connection to Spark master...")
        import socket
        try:
            host = spark_master.replace('spark://', '').split(':')[0]
            port = int(spark_master.split(':')[-1])
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                print(f"✓ Successfully connected to {host}:{port}")
            else:
                print(f"✗ Cannot connect to {host}:{port}")
                return False
        except Exception as e:
            print(f"✗ Network connection failed: {e}")
            return False
        
        # Clean up any existing Spark context
        print("Cleaning up existing Spark contexts...")
        if SparkContext._active_spark_context is not None:
            print("Found existing SparkContext, stopping it...")
            SparkContext._active_spark_context.stop()
            time.sleep(3)
          # Create SparkSession with minimal configuration
        print("Creating SparkSession...")
        spark = SparkSession.builder \
            .appName("SparkConnectionTest") \
            .master(spark_master) \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.network.timeout", "300s") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=default") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=default") \
            .getOrCreate()
        
        print("✓ SparkSession created successfully")
        print(f"Spark version: {spark.version}")
        print(f"Application ID: {spark.sparkContext.applicationId}")
        
        # Test basic functionality
        print("Testing basic Spark functionality...")
        test_data = [(1, "hello"), (2, "world")]
        df = spark.createDataFrame(test_data, ["id", "value"])
        count = df.count()
        print(f"✓ Basic DataFrame test passed, count: {count}")
        
        # Clean up
        print("Stopping SparkSession...")
        spark.stop()
        print("✓ SparkSession stopped successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_spark_connection()
    if success:
        print("\n✓ All Spark tests passed!")
    else:
        print("\n✗ Spark tests failed!")
        sys.exit(1)
