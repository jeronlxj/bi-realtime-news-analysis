from kafka import KafkaProducer
import json
import time

class ETLPipeline:
    def __init__(self, kafka_topic, kafka_bootstrap_servers):
        self.kafka_topic = kafka_topic
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def collect_data(self):
        # Simulate collecting news exposure logs
        # In a real scenario, this would interface with a data source
        for i in range(10):  # Simulating 10 data points
            data = {
                'news_id': i,
                'exposure_time': time.time(),
                'user_id': f'user_{i % 5}',  # Simulating 5 users
                'click': i % 2 == 0  # Simulating click behavior
            }
            self.process_data(data)

    def process_data(self, data):
        # Process the data as needed
        # For now, we will just send it to Kafka
        self.send_to_kafka(data)

    def send_to_kafka(self, data):
        self.producer.send(self.kafka_topic, value=data)
        print(f"Sent data to Kafka: {data}")

    def run(self):
        self.collect_data()
        self.producer.flush()  # Ensure all messages are sent

if __name__ == "__main__":
    kafka_topic = 'news_exposure_logs'
    kafka_bootstrap_servers = 'localhost:9092'
    etl_pipeline = ETLPipeline(kafka_topic, kafka_bootstrap_servers)
    etl_pipeline.run()