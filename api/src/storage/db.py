from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, JSON, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging
from typing import Dict, List

Base = declarative_base()

class News(Base):
    """News article table schema"""
    __tablename__ = 'news'
    
    news_id = Column(String, primary_key=True)
    category = Column(String)
    topic = Column(String)
    headline = Column(String)
    news_body = Column(Text)
    title_entity = Column(JSON)
    entity_content = Column(JSON)

class ExposureLog(Base):
    """News exposure log table schema"""
    __tablename__ = 'exposure_logs'
    
    impression_id = Column(String, primary_key=True)
    user_id = Column(String)
    timestamp = Column(DateTime)
    news_id = Column(String)
    category = Column(String)
    headline = Column(String)
    topic = Column(String)
    clicked = Column(Integer)
    dwell_time = Column(Float)
    processed_timestamp = Column(DateTime)

class DatabaseConnection:
    def __init__(self):
        """Initialize database connection"""
        # Use PostgreSQL as the storage system
        self.engine = create_engine('postgresql://postgres:postgres@localhost:5432/news_analysis')
        
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        
        # Create session factory
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
    
    def store_news_data(self, news_records: List[Dict]):
        """Store PENS news data in the database"""
        try:
            for record in news_records:
                news = News(
                    news_id=record['News ID'],
                    category=record['Category'],
                    topic=record['Topic'],
                    headline=record['Headline'],
                    news_body=record['News body'],
                    title_entity=record['Title entity'],
                    entity_content=record['Entity content']
                )
                self.session.merge(news)  # Use merge to handle updates of existing records
            
            self.session.commit()
            self.logger.info(f"Successfully stored {len(news_records)} news articles")
            
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error storing news data: {e}")
            raise
    
    def store_exposure_log(self, log: Dict):
        """Store a single exposure log in the database"""
        try:
            exposure_log = ExposureLog(
                impression_id=log['impression_id'],
                user_id=log['user_id'],
                timestamp=datetime.fromisoformat(log['timestamp']),
                news_id=log['news_id'],
                category=log['category'],
                headline=log['headline'],
                topic=log['topic'],
                clicked=log['clicked'],
                dwell_time=log['dwell_time'],
                processed_timestamp=datetime.fromisoformat(log['processed_timestamp'])
            )
            
            self.session.merge(exposure_log)
            self.session.commit()
            
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error storing exposure log: {e}")
            raise
    
    def close(self):
        """Close the database session"""
        self.session.close()