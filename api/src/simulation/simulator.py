from datetime import datetime, timedelta
import random
import json
import pandas as pd
import os
from typing import List, Dict

class NewsSimulator:
    def __init__(self):
        """Initialize simulator with PENS dataset files"""
        # Add impression ID counter
        self.current_impression_id = 1
        
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        self.news_file = os.path.join(base_dir, 'data', 'PENS', 'news.tsv')
        self.train_file = os.path.join(base_dir, 'data', 'PENS', 'train.tsv')
        
        try:
            # Load news data
            self.news_df = pd.read_csv(self.news_file, sep='\t')
            print(f"Loaded {len(self.news_df)} news articles")
            
            # Create news lookup dictionary for faster access
            self.news_lookup = self.news_df.set_index('News ID').to_dict('index')
            
            # Read train.tsv in chunks to handle large file
            self.chunk_size = 1000
            self.train_reader = pd.read_csv(
                self.train_file, 
                sep='\t',
                chunksize=self.chunk_size,
                names=['UserID', 'ClicknewsID', 'dwelltime', 'exposure_time', 
                      'pos', 'neg', 'start', 'end', 'dwelltime_pos']
            )
            
        except Exception as e:
            print(f"Error loading PENS data: {e}")
            print(f"Looking for files in: {self.news_file}")
            raise

    def parse_impression(self, row: pd.Series) -> List[Dict]:
        """Parse a single impression row into multiple log entries"""
        logs = []
        
        # Get the current impression ID for this group of logs
        impression_id = f"imp_{self.current_impression_id:07d}"  # Zero-padded 7-digit number
        self.current_impression_id += 1
        
        # Parse clicked (positive) news
        pos_news = row['pos'].split()
        pos_dwell_times = [int(t) for t in row['dwelltime_pos'].split()]
        
        # Parse unclicked (negative) news
        neg_news = row['neg'].split()
        
        # Convert start/end times to datetime
        start_time = pd.to_datetime(row['start'])
        end_time = pd.to_datetime(row['end'])
        
        # Generate logs for clicked news
        for news_id, dwell_time in zip(pos_news, pos_dwell_times):
            if news_id in self.news_lookup:
                news = self.news_lookup[news_id]
                logs.append({
                    'impression_id': impression_id,
                    'user_id': row['UserID'],
                    'timestamp': start_time.isoformat(),
                    'news_id': news_id,
                    'category': news.get('Category', ''),
                    'headline': news.get('Headline', ''),
                    'topic': news.get('Topic', ''),
                    'clicked': 1,
                    'dwell_time': dwell_time
                })
        
        # Generate logs for unclicked news
        for news_id in neg_news:
            if news_id in self.news_lookup:
                news = self.news_lookup[news_id]
                logs.append({
                    'impression_id': impression_id,
                    'user_id': row['UserID'],
                    'timestamp': start_time.isoformat(),
                    'news_id': news_id,
                    'category': news.get('Category', ''),
                    'headline': news.get('Headline', ''),
                    'topic': news.get('Topic', ''),
                    'clicked': 0,
                    'dwell_time': 0
                })
        
        return logs

    def simulate_exposure_logs(self, num_logs=100):
        """Generate exposure logs from actual training data"""
        logs = []
        rows_processed = 0
        
        # Get next chunk of training data
        for chunk in self.train_reader:
            for _, row in chunk.iterrows():
                chunk_logs = self.parse_impression(row)
                logs.extend(chunk_logs)
                
                rows_processed += 1
                if rows_processed >= num_logs:
                    break
            
            if rows_processed >= num_logs:
                break
                
        return logs[:num_logs]  # Return exactly num_logs entries

    def save_logs_to_file(self, filename='exposure_logs.json', batch_size=100):
        """Save simulated logs to a file"""
        logs = self.simulate_exposure_logs(batch_size)
        with open(filename, 'a') as f:
            for log in logs:
                f.write(json.dumps(log) + '\n')
        print(f"Saved {len(logs)} logs to {filename}")

if __name__ == "__main__":
    simulator = NewsSimulator()
    simulator.save_logs_to_file()