from datetime import datetime, timedelta
import random
import json
import pandas as pd
import os, time
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
            # Check if files exist and print their sizes
            for file_path in [self.news_file, self.train_file]:
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"File not found: {file_path}")
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                print(f"Found {os.path.basename(file_path)} (Size: {size_mb:.2f} MB)")
            
            # # Load news data
            # print("\nLoading news data...")
            # self.news_df = pd.read_csv(self.news_file, sep='\t')
            # print(f"Loaded {len(self.news_df)} news articles")
            
            # # Print first few rows of news data for verification
            # print("\nFirst few rows of news data:")
            # print(self.news_df.head())
            # print("\nNews columns:", self.news_df.columns.tolist())
            
            # # Create news lookup dictionary for faster access
            # self.news_lookup = self.news_df.set_index('News ID').to_dict('index')
            
            # Read train.tsv in chunks to handle large file
            print("\nLoading training data...")
            self.chunk_size = 1000
            self.train_reader = pd.read_csv(
                self.train_file, 
                sep='\t',
                chunksize=self.chunk_size
            )
            
            # Try to read first chunk to verify train data
            first_chunk = next(self.train_reader)
            print(f"\nFirst few rows of train data:")
            print(first_chunk.head())
            print("\nTrain columns:", first_chunk.columns.tolist())
            
            # Reset train reader for actual use
            self.train_reader = pd.read_csv(
                self.train_file, 
                sep='\t',
                chunksize=self.chunk_size
            )
            
        except FileNotFoundError as e:
            print(f"File not found error: {e}")
            print(f"Please ensure the PENS dataset files are in the correct location:")
            print(f"- News file should be at: {self.news_file}")
            print(f"- Train file should be at: {self.train_file}")
            raise
        except pd.errors.EmptyDataError:
            print(f"Error: One of the data files is empty")
            raise
        except Exception as e:
            print(f"Error loading PENS data: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            print(f"Looking for files in:")
            print(f"- News file: {self.news_file}")
            print(f"- Train file: {self.train_file}")
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
            # if news_id in self.news_lookup:
            #     news = self.news_lookup[news_id]
                logs.append({
                    'impression_id': impression_id,
                    'user_id': row['UserID'],
                    'timestamp': start_time.isoformat(),
                    'news_id': news_id,
                    # 'category': news.get('Category', ''),
                    # 'headline': news.get('Headline', ''),
                    # 'topic': news.get('Topic', ''),
                    'clicked': 1,
                    'dwell_time': dwell_time
                })
        
        # Generate logs for unclicked news
        for news_id in neg_news:
            # if news_id in self.news_lookup:
            #     news = self.news_lookup[news_id]
                logs.append({
                    'impression_id': impression_id,
                    'user_id': row['UserID'],
                    'timestamp': start_time.isoformat(),
                    'news_id': news_id,
                    # 'category': news.get('Category', ''),
                    # 'headline': news.get('Headline', ''),
                    # 'topic': news.get('Topic', ''),
                    'clicked': 0,
                    'dwell_time': 0
                })
        
        return logs

    #400,000 rows in train.tsv, 100,000 rows in valid.tsv
    def simulate_exposure_logs(self, num_rows=100): 
        """Generate exposure logs from actual training data"""
        logs = []
        rows_processed = 0
        
        # Get next chunk of training data
        for chunk in self.train_reader:
            for _, row in chunk.iterrows():
                chunk_logs = self.parse_impression(row)
                logs.extend(chunk_logs)
                
                rows_processed += 1
                if rows_processed >= num_rows:
                    break
            
            if rows_processed >= num_rows:
                break
                
        return logs  # Return logs for exactly num_rows entries

    def save_logs_to_file(self, filename='exposure_logs.json', batch_size=100):
        """Save simulated logs to a file"""
        logs = self.simulate_exposure_logs(batch_size)
        with open(filename, 'a') as f:
            for log in logs:
                f.write(json.dumps(log) + '\n')
        print(f"Saved {len(logs)} logs to {filename}")
    
    def generate_continuous_logs(self, interval_seconds: int = 5):
        """Generate logs continuously at specified intervals for Flume to collect
        
        Args:
            interval_seconds: Time between log generation batches
        """
        import time
        
        # Use current directory (simulation directory) for output
        current_dir = os.path.dirname(__file__)
        logs_dir = os.path.join(current_dir, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        while True:
            try:
                # Generate timestamp for this batch
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = os.path.join(logs_dir, f'exposure_logs_{timestamp}.json')
                
                # Generate and save a batch of logs
                self.save_logs_to_file(filename=filename, batch_size=100)
                print(f"Generated logs at {filename}")
                
                # Wait for next interval
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                print("Stopping log generation...")
                break
            except Exception as e:
                print(f"Error generating logs: {e}")
                raise

if __name__ == "__main__":
    simulator = NewsSimulator()
    simulator.save_logs_to_file()