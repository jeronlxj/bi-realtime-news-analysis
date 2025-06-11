"""
Spark Session Manager for handling SparkContext and SparkSession lifecycle
"""
import os
import time
import logging
from typing import Optional
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

class SparkManager:
    """Manages Spark session lifecycle with proper error handling"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark_session: Optional[SparkSession] = None
        
    def create_spark_session(self, app_name: str = "NewsAnalysisApp", max_retries: int = 3) -> SparkSession:
        """
        Create a new SparkSession with proper configuration and error handling
        
        Args:
            app_name: Name of the Spark application
            max_retries: Maximum number of retry attempts
            
        Returns:
            SparkSession instance
            
        Raises:
            Exception: If unable to create SparkSession after all retries
        """
        spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark:7077')
        
        for attempt in range(max_retries):
            try:
                # Stop any existing SparkContext/Session
                self._cleanup_existing_context()
                
                # Configure Spark
                conf = SparkConf()
                conf.setAppName(app_name)
                conf.setMaster(spark_master)
                
                # Add configurations for reliability and performance
                conf.set("spark.sql.adaptive.enabled", "true")
                conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                conf.set("spark.driver.memory", "1g")
                conf.set("spark.executor.memory", "1g")
                conf.set("spark.driver.maxResultSize", "500m")
                conf.set("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
                conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                conf.set("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
                
                # Network and timeout configurations
                conf.set("spark.network.timeout", "300s")
                conf.set("spark.rpc.askTimeout", "300s")
                conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")  # Disable Arrow for compatibility
                
                # Create SparkSession
                self.spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
                
                # Set log level to reduce noise
                self.spark_session.sparkContext.setLogLevel("WARN")
                
                # Test the connection
                self._test_spark_connection()
                
                self.logger.info(f"Successfully created SparkSession with master: {spark_master}")
                self.logger.info(f"Spark version: {self.spark_session.version}")
                self.logger.info(f"Spark application ID: {self.spark_session.sparkContext.applicationId}")
                
                return self.spark_session
                
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed to create SparkSession: {str(e)}")
                if attempt == max_retries - 1:
                    self.logger.error("Failed to create SparkSession after all retries")
                    raise Exception(f"Cannot connect to Spark master at {spark_master}: {str(e)}")
                time.sleep(5)  # Wait before retry
        
        raise Exception("Unexpected error in SparkSession creation")
    
    def _cleanup_existing_context(self):
        """Clean up any existing SparkContext or SparkSession"""
        try:
            # Stop existing SparkSession if it exists
            if self.spark_session:
                self.spark_session.stop()
                self.spark_session = None
                self.logger.info("Stopped existing SparkSession")
            
            # Stop existing SparkContext if it exists
            if SparkContext._active_spark_context is not None:
                SparkContext._active_spark_context.stop()
                self.logger.info("Stopped existing SparkContext")
                
            # Wait for cleanup
            time.sleep(2)
            
        except Exception as e:
            self.logger.warning(f"Error during Spark cleanup: {str(e)}")
    
    def _test_spark_connection(self):
        """Test the Spark connection by running a simple operation"""
        try:
            # Create a simple DataFrame to test the connection
            test_data = [(1, "test"), (2, "connection")]
            df = self.spark_session.createDataFrame(test_data, ["id", "value"])
            count = df.count()
            self.logger.info(f"Spark connection test successful. Test DataFrame count: {count}")
            
        except Exception as e:
            self.logger.error(f"Spark connection test failed: {str(e)}")
            raise
    
    def get_session(self) -> Optional[SparkSession]:
        """Get the current SparkSession"""
        return self.spark_session
    
    def is_session_active(self) -> bool:
        """Check if the SparkSession is active and healthy"""
        if not self.spark_session:
            return False
        
        try:
            # Try to access SparkContext to check if it's still active
            sc = self.spark_session.sparkContext
            if sc._jsc is None:
                return False
            return True
        except Exception:
            return False
    
    def stop_session(self):
        """Stop the current SparkSession"""
        if self.spark_session:
            try:
                self.spark_session.stop()
                self.logger.info("SparkSession stopped successfully")
            except Exception as e:
                self.logger.error(f"Error stopping SparkSession: {str(e)}")
            finally:
                self.spark_session = None
    
    def restart_session(self, app_name: str = "NewsAnalysisApp") -> SparkSession:
        """Restart the SparkSession"""
        self.logger.info("Restarting SparkSession...")
        self.stop_session()
        return self.create_spark_session(app_name)
    
    def __del__(self):
        """Destructor to ensure resources are cleaned up"""
        try:
            self.stop_session()
        except:
            pass

# Global instance for singleton pattern
_spark_manager = None

def get_spark_manager() -> SparkManager:
    """Get the global SparkManager instance"""
    global _spark_manager
    if _spark_manager is None:
        _spark_manager = SparkManager()
    return _spark_manager
