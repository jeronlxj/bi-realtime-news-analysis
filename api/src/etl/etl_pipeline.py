from kafka import KafkaProducer, KafkaConsumer
from threading import Thread
import json
import pandas as pd
import os
from typing import Dict, List
import logging
from datetime import datetime

class NewsETLPipeline:
    def __init__(self):
        """Initialize ETL pipeline with Kafka configuration"""
        kafka_host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_host],
            client_id='news_etl_pipeline',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize database connection
        self.setup_storage()

    def setup_storage(self):
        """Initialize connection to the storage system"""
        from storage.db import DatabaseConnection
        self.db = DatabaseConnection()
        
    def load_pens_news_data(self):
        """Load and process PENS news data"""
        ### Check if running in Docker (where /data is mounted) or locally ###
        if os.path.exists('/data'):
            # Docker environment
            base_dir = '/data'
        else:
            # Local environment
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            base_dir = os.path.join(base_dir, 'data')
        news_file = os.path.join(base_dir, 'PENS', 'news.tsv')
        
        self.logger.info(f"Starting to load PENS news data from: {news_file}")

        try:
            # Load news data
            chunk_size = 10000  # Adjust based on your memory constraints
            chunks = []

            for chunk in pd.read_csv(news_file, sep='\t', chunksize=chunk_size):
                self.logger.info(f"Processing chunk of size {len(chunk)}")
                chunks.append(chunk)

            news_df = pd.concat(chunks)
            self.logger.info(f"Loaded {len(news_df)} news articles from PENS dataset")
            
            # Store news data in the database
            news_records = news_df.to_dict('records')
            batch_size = 1000
            for i in range(0, len(news_records), batch_size):
                batch = news_records[i:i + batch_size]
                self.logger.info(f"Storing batch {i//batch_size + 1} of {len(news_records)//batch_size + 1}")
                self.db.store_news_data(batch)
            self.logger.info("Successfully stored news data in database")
            
        except Exception as e:
            self.logger.error(f"Error loading PENS news data: {e}")
            raise

    # not used
    def process_exposure_log(self, log: Dict):
        """Process a single exposure log"""
        try:
            # Enrich log with additional information if needed
            enriched_log = {
                **log,
                'processed_timestamp': datetime.now().isoformat()
            }
            
            # Send to Kafka
            future = self.producer.send(
                'news_exposure_logs',
                enriched_log  # value_serializer will handle JSON encoding
            )
            # Wait for the message to be delivered
            future.get(timeout=10)
            
        except Exception as e:
            self.logger.error(f"Error processing exposure log: {e}")
            raise

    def delivery_report(self, err, msg):
        """Callback for Kafka producer to report delivery result"""
        if err is not None:
            self.logger.error(f'Message delivery failed: {err}')
        else:
            self.logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def consume_and_store_logs(self):
        """Consume logs from Kafka and store in database"""
        kafka_host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        consumer = KafkaConsumer(
            'news_exposure_logs',
            bootstrap_servers=[kafka_host],
            group_id='news_storage_group',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        try:
            for message in consumer:
                # Process and store the message
                log = message.value # Note value_deserializer already decoded the message
                # Add processing timestamp before storing
                log['processed_timestamp'] = datetime.now().isoformat()
                # self.logger.debug(f"Processing message: {log}")
                self.db.store_exposure_log(log)
                
        except KeyboardInterrupt:
            self.logger.info('Stopping consumer...')
        finally:
            consumer.close()

    def run_pipeline(self):
        """Run the complete ETL pipeline"""
        try:
            # Load PENS data in a separate thread
            pens_loading_thread = Thread(target=self.load_pens_news_data)
            pens_loading_thread.start()
            
            # Start consuming and storing logs
            self.logger.info("Starting to consume exposure logs...")
            self.consume_and_store_logs()

            # Wait for PENS data loading to complete
            pens_loading_thread.join()
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise
        finally:
            # Clean up resources
            self.producer.close()

if __name__ == "__main__":
    pipeline = NewsETLPipeline()
    pipeline.run_pipeline()