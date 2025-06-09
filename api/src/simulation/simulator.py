from datetime import datetime, timedelta
import random
import json

class NewsSimulator:
    def __init__(self, num_articles=10):
        self.num_articles = num_articles
        self.articles = self.generate_articles()

    def generate_articles(self):
        articles = []
        for i in range(self.num_articles):
            article = {
                'id': i,
                'title': f'News Article {i}',
                'content': f'This is the content of news article {i}.',
                'timestamp': datetime.now() - timedelta(minutes=random.randint(0, 60)),
                'exposure': random.randint(1, 1000),
                'clicks': random.randint(0, 100)
            }
            articles.append(article)
        return articles

    def simulate_exposure_logs(self):
        logs = []
        for article in self.articles:
            log_entry = {
                'article_id': article['id'],
                'timestamp': article['timestamp'].isoformat(),
                'exposure': article['exposure'],
                'clicks': article['clicks']
            }
            logs.append(log_entry)
        return logs

    def save_logs_to_file(self, filename='exposure_logs.json'):
        logs = self.simulate_exposure_logs()
        with open(filename, 'a') as f:  # append mode
            for log in logs:
                f.write(json.dumps(log) + '\n')

if __name__ == "__main__":
    simulator = NewsSimulator()
    simulator.save_logs_to_file()