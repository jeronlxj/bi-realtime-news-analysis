from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, JSON, Text, Index, Boolean, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import logging
import os
from typing import Dict, List, Optional
import time
import json

Base = declarative_base()

def make_json_serializable(obj):
    """Convert objects to JSON serializable format"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [make_json_serializable(item) for item in obj]
    else:
        return obj

# news first 4 cols eg
#"N89995","travel","traveltripideas","The Most Scenic Drive in Every State"
class News(Base):
    """News article table schema with optimized indexing"""
    __tablename__ = 'news'
    
    news_id = Column(String, primary_key=True)
    category = Column(String, index=True)  # Index for category-based queries
    topic = Column(String, index=True)     # Index for topic-based queries
    headline = Column(String)
    news_body = Column(Text)
    title_entity = Column(JSON)
    entity_content = Column(JSON)
    
    # Create composite indexes for common query patterns
    __table_args__ = (
        Index('idx_category_topic', 'category', 'topic'),
    )

# exposure log eg 
#"imp_0000001","U335175","N55476","2019-07-03 06:43:49",1,34,"2025-06-13 00:36:36.867769"
class ExposureLog(Base):
    """News exposure log table schema with optimized indexing"""
    __tablename__ = 'exposure_logs'
    
    impression_id = Column(String, primary_key=True) # Part of composite primary key
    user_id = Column(String, primary_key=True)  # Part of composite primary key
    news_id = Column(String, primary_key=True)  # Part of composite primary key
    timestamp = Column(DateTime, index=True)  # Index for time-based queries
    clicked = Column(Integer, index=True)  # Index for click analysis
    dwell_time = Column(Float)
    processed_timestamp = Column(DateTime)
    
    # Create composite indexes for common query patterns
    __table_args__ = (
        Index('idx_user_timestamp', 'user_id', 'timestamp'),
        Index('idx_news_timestamp', 'news_id', 'timestamp'),
        Index('idx_clicked_timestamp', 'clicked', 'timestamp'),
        Index('idx_user_clicked', 'user_id', 'clicked'),
        Index('idx_news_clicked_timestamp', 'news_id', 'clicked', 'timestamp'),
    )

# query log eg
#121,"performance_stats","{""hours"": 24}",0.002145528793334961,3,"2025-06-13 01:00:51.221505",true,[null]
class QueryLog(Base):
    """Query performance tracking table"""
    __tablename__ = 'query_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    query_type = Column(String, index=True)  # Type of query executed
    query_params = Column(JSON)  # Parameters used in the query
    execution_time = Column(Float)  # Query execution time in seconds
    result_count = Column(Integer)  # Number of results returned
    timestamp = Column(DateTime, default=datetime.now, index=True)
    success = Column(Boolean, default=True, index=True)
    error_message = Column(Text, nullable=True)
    
    __table_args__ = (
        Index('idx_query_type_timestamp', 'query_type', 'timestamp'),
        Index('idx_execution_time', 'execution_time'),
    )

class DatabaseConnection:
    def __init__(self):
        """Initialize database connection with optimized settings"""
        # Use PostgreSQL as the storage system
        # Check if running in Docker (POSTGRES_HOST env var is set)
        postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
        postgres_port = os.getenv('POSTGRES_PORT', '15432' if postgres_host == 'localhost' else '5432')
        
        db_url = f'postgresql://newsuser:newspass@{postgres_host}:{postgres_port}/newsdb'
        
        # Enhanced connection settings for performance
        self.engine = create_engine(
            db_url,
            pool_size=20,  # Number of connections to maintain in pool
            max_overflow=30,  # Additional connections allowed beyond pool_size
            pool_timeout=30,  # Timeout for getting connection from pool
            pool_recycle=3600,  # Recycle connections every hour
            echo=False  # Set to True for SQL debugging
        )
        
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        
        # Create session factory
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.info("Database connection initialized with performance optimizations")
    
    def store_news_data(self, news_records: List[Dict]):
        """Store PENS news data in the database"""
        # Create new session for this operation
        Session = sessionmaker(bind=self.engine)
        local_session = Session()
        
        try:
            news_objects = []
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
                news_objects.append(news)
            
            # Perform individual merges in a transaction
            for news in news_objects:
                local_session.merge(news)
                
            # Commit all changes at once
            local_session.commit()
            self.logger.info(f"Successfully stored {len(news_records)} news articles")
            
        except Exception as e:
            # Only rollback if the session is still active
            if local_session.is_active:
                local_session.rollback()
            self.logger.error(f"Error storing news data: {e}")
            raise
        finally:
            local_session.close()
    
    def store_exposure_log(self, log: Dict):
        """Store a single exposure log in the database"""
        try:
            # self.logger.info(f"Attempting to store log: {log['impression_id']}")

            exposure_log = ExposureLog(
                impression_id=log['impression_id'],
                user_id=log['user_id'],
                timestamp=datetime.fromisoformat(log['timestamp']),
                news_id=log.get('news_id', ''),
                # category=log.get('category', ''),
                # headline=log.get('headline', ''),
                # topic=log.get('topic', ''),
                clicked=log['clicked'],
                dwell_time=log['dwell_time'],
                processed_timestamp=datetime.fromisoformat(log['processed_timestamp'])
            )
            
            self.session.merge(exposure_log)
            # self.session.flush()  # Force SQL execution before commit
        
            # Verify the record exists
            verification = self.session.query(ExposureLog).filter_by(
                impression_id=log['impression_id']
            ).first()
            # self.logger.info(f"Verification query result: {vars(verification) if verification else 'Not found'}")
            
            self.session.commit()
            # self.logger.info(f"Successfully committed log: {log['impression_id']}")
            
        except Exception as e:
            # Only rollback if the session is still active
            if self.session.is_active:
                self.session.rollback()
            self.logger.error(f"Error storing exposure log: {e}")
            raise
    def log_query_performance(self, query_type: str, query_params: Dict, 
                            execution_time: float, result_count: int, 
                            success: bool = True, error_message: str = None):
        """Log query performance for monitoring and optimization"""
        try:
            # Make query_params JSON serializable
            serializable_params = make_json_serializable(query_params)
            
            query_log = QueryLog(
                query_type=query_type,
                query_params=serializable_params,
                execution_time=execution_time,
                result_count=result_count,
                success=success,
                error_message=error_message
            )
            self.session.add(query_log)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error logging query performance: {e}")
    
    def execute_timed_query(self, query_type: str, query_func, **kwargs):
        """Execute a query with timing and logging"""
        start_time = time.time()
        result = None
        result_count = 0
        success = True
        error_message = None
        
        try:
            result = query_func(**kwargs)
            if hasattr(result, '__len__'):
                result_count = len(result)
            elif hasattr(result, 'count'):
                result_count = result.count()
            else:
                result_count = 1 if result else 0
                
        except Exception as e:
            success = False
            error_message = str(e)
            self.logger.error(f"Query failed: {query_type} - {error_message}")
            raise
        finally:
            execution_time = time.time() - start_time
            self.log_query_performance(
                query_type=query_type,
                query_params=kwargs,
                execution_time=execution_time,
                result_count=result_count,
                success=success,
                error_message=error_message
            )
            
        return result
    
    # High-performance query methods for the required analytics
    
    def get_news_lifecycle(self, news_id: str, start_date: Optional[datetime] = None, 
                          end_date: Optional[datetime] = None) -> List[Dict]:
        """Query the lifecycle of a single news article - shows popularity changes over time"""
        def _query(news_id, start_date, end_date):
            query = self.session.query(
                func.date_trunc('hour', ExposureLog.timestamp),
                func.count(func.distinct(ExposureLog.impression_id)).label('total_impressions'),
                func.sum(ExposureLog.clicked).label('total_clicks'),
                func.avg(ExposureLog.dwell_time).label('avg_dwell_time'),
                func.count(func.distinct(ExposureLog.user_id)).label('unique_users')
            ).filter(ExposureLog.news_id == news_id)

            query = query.filter(ExposureLog.timestamp.isnot(None))

            if start_date:
                query = query.filter(ExposureLog.timestamp >= start_date)
            if end_date:
                query = query.filter(ExposureLog.timestamp <= end_date)
            
            # Group by hour for time series analysis          
            results = query.group_by(
                func.date_trunc('hour', ExposureLog.timestamp)
            ).order_by(func.date_trunc('hour', ExposureLog.timestamp)).all()
            
            return [{
                'timestamp': result.timestamp.isoformat() if result.timestamp else None,
                'total_impressions': result.total_impressions,
                'total_clicks': result.total_clicks or 0,
                'avg_dwell_time': float(result.avg_dwell_time) if result.avg_dwell_time else 0,
                'unique_users': result.unique_users,
                'click_rate': (result.total_clicks or 0) / max(result.total_impressions, 1)
            } for result in results]
        
        return self.execute_timed_query(
            'news_lifecycle', _query, 
            news_id=news_id, start_date=start_date, end_date=end_date        )
    
    def get_category_trends(self, start_date: Optional[datetime] = None,
                           end_date: Optional[datetime] = None) -> List[Dict]:
        """Statistical query on changes of news categories over time based on exposure data"""
        def _query(start_date, end_date):
            # Join with News table to get category information
            # Use exposure timestamp since news doesn't have created_at
            query = self.session.query(
                News.category,
                func.date_trunc('day', ExposureLog.timestamp).label('date'),
                func.count(func.distinct(ExposureLog.impression_id)).label('impressions'),
                func.sum(ExposureLog.clicked).label('clicks'),
                func.count(func.distinct(ExposureLog.user_id)).label('unique_users'),
                func.count(func.distinct(ExposureLog.news_id)).label('unique_news')
            ).join(News, ExposureLog.news_id == News.news_id)
            
            if start_date:
                query = query.filter(ExposureLog.timestamp >= start_date)
            if end_date:
                query = query.filter(ExposureLog.timestamp <= end_date)
            
            results = query.group_by(
                News.category, 
                func.date_trunc('day', ExposureLog.timestamp)
            ).order_by((func.sum(ExposureLog.clicked) * 1.0 / func.count(func.distinct(ExposureLog.impression_id))).desc()).all()
            
            return [{
                'category': result.category,
                'date': result.date.isoformat() if result.date else None,
                'impressions': result.impressions,
                'clicks': result.clicks or 0,
                'unique_users': result.unique_users,
                'unique_news': result.unique_news,
                'click_rate': (result.clicks or 0) / max(result.impressions, 1)
            } for result in results]
        
        return self.execute_timed_query(
            'category_trends', _query,
            start_date=start_date, end_date=end_date
        )
    
    def get_user_interest_changes(self, user_id: Optional[str] = None,
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None) -> List[Dict]:
        """Statistical query on changes in user interests"""
        def _query(user_id, start_date, end_date):
            query = self.session.query(
                ExposureLog.user_id,
                News.category,
                News.topic,
                func.count(func.distinct(ExposureLog.impression_id)).label('impressions'),
                func.sum(ExposureLog.clicked).label('clicks'),
                func.avg(ExposureLog.dwell_time).label('avg_dwell_time')
            ).join(News, ExposureLog.news_id == News.news_id)
            
            if user_id:
                query = query.filter(ExposureLog.user_id == user_id)
            if start_date:
                query = query.filter(ExposureLog.timestamp >= start_date)
            if end_date:
                query = query.filter(ExposureLog.timestamp <= end_date)
            
            results = query.group_by(
                ExposureLog.user_id, News.category, News.topic
            ).having(func.count(func.distinct(ExposureLog.impression_id)) > 0).all()
            
            return [{
                'user_id': result.user_id,
                'category': result.category,
                'topic': result.topic,
                'impressions': result.impressions,
                'clicks': result.clicks or 0,
                'avg_dwell_time': float(result.avg_dwell_time) if result.avg_dwell_time else 0,
                'engagement_score': ((result.clicks or 0) * 2 + 
                                   (float(result.avg_dwell_time) if result.avg_dwell_time else 0) / 30) / max(result.impressions, 1)
            } for result in results]
        
        return self.execute_timed_query(
            'user_interest_changes', _query,
            user_id=user_id, start_date=start_date, end_date=end_date
        )
    
    def get_hot_news_prediction(self, hours_ahead: int = 24, min_impressions: int = 50, 
                               reference_date: Optional[datetime] = None) -> List[Dict]:
        """Analyze what kind of news is most likely to become hot news"""
        def _query(hours_ahead, min_impressions, reference_date):
            # Use provided reference date or default to current time
            # This allows testing with historical dataset
            ref_date = reference_date if reference_date else datetime.now()
            
            # Calculate recent performance metrics
            recent_cutoff = ref_date - timedelta(hours=hours_ahead)
            
            query = self.session.query(
                News.news_id,
                News.category,
                News.topic,
                News.headline,
                News.news_body,  # Added news_body field to the query
                func.count(func.distinct(ExposureLog.impression_id)).label('impressions'),
                func.sum(ExposureLog.clicked).label('clicks'),
                func.count(func.distinct(ExposureLog.user_id)).label('unique_users'),
                func.avg(ExposureLog.dwell_time).label('avg_dwell_time'),
                (func.sum(ExposureLog.clicked) * 1.0 / func.count(func.distinct(ExposureLog.impression_id))).label('click_rate'),
                (func.count(func.distinct(ExposureLog.user_id)) * 1.0 / func.count(func.distinct(ExposureLog.impression_id))).label('user_diversity')
            ).join(ExposureLog, News.news_id == ExposureLog.news_id).filter(
                ExposureLog.timestamp >= recent_cutoff
            ).group_by(
                News.news_id, News.category, News.topic, News.headline, News.news_body  # Added news_body to GROUP BY
            ).having(
                func.count(func.distinct(ExposureLog.impression_id)) >= min_impressions
            )
            
            results = query.order_by(
                (func.sum(ExposureLog.clicked) * 1.0 / func.count(func.distinct(ExposureLog.impression_id))).desc()
            ).limit(50).all()
            
            return [{
                'news_id': result.news_id,
                'category': result.category,
                'topic': result.topic,
                'headline': result.headline,
                'news_body': result.news_body,  # Include news_body in the result
                'impressions': result.impressions,
                'clicks': result.clicks or 0,
                'unique_users': result.unique_users,
                'avg_dwell_time': float(result.avg_dwell_time) if result.avg_dwell_time else 0,
                'click_rate': float(result.click_rate) if result.click_rate else 0,
                'user_diversity': float(result.user_diversity) if result.user_diversity else 0,
                'hotness_score': (
                    (float(result.click_rate) if result.click_rate else 0) * 0.4 +
                    (float(result.user_diversity) if result.user_diversity else 0) * 0.3 +
                    min((float(result.avg_dwell_time) if result.avg_dwell_time else 0) / 100, 1) * 0.3
                )
            } for result in results]
        
        return self.execute_timed_query(
            'hot_news_prediction', _query,
            hours_ahead=hours_ahead, min_impressions=min_impressions, reference_date=reference_date
        )
    
    def get_user_recommendations(self, user_id: str, limit: int = 10, 
                               reference_date: Optional[datetime] = None) -> List[Dict]:
        """Real-time news recommendations based on user's browsing history"""
        def _query(user_id, limit, reference_date):
            # Get user's preferred categories and topics based on click history
            user_prefs = self.session.query(
                News.category,
                News.topic,
                func.count(ExposureLog.clicked).label('clicks')
            ).join(ExposureLog, News.news_id == ExposureLog.news_id).filter(
                ExposureLog.user_id == user_id,
                ExposureLog.clicked == 1
            ).group_by(News.category, News.topic).order_by(
                func.count(ExposureLog.clicked).desc()
            ).limit(5).all()
            
            if not user_prefs:
                # If no click history, return trending news
                return self.get_hot_news_prediction(hours_ahead=6, min_impressions=50, reference_date=reference_date)[:limit]
            
            # Find similar news in preferred categories/topics that user hasn't seen
            seen_news = self.session.query(ExposureLog.news_id).filter(
                ExposureLog.user_id == user_id
            ).subquery()
            
            pref_categories = [pref.category for pref in user_prefs]
            pref_topics = [pref.topic for pref in user_prefs]
            
            query = self.session.query(
                News.news_id,
                News.category,
                News.topic,
                News.headline,
                func.count(func.distinct(ExposureLog.impression_id)).label('popularity'),
                func.avg(ExposureLog.clicked * 1.0).label('avg_click_rate')
            ).join(ExposureLog, News.news_id == ExposureLog.news_id).filter(
                News.category.in_(pref_categories),
                News.topic.in_(pref_topics),
                ~News.news_id.in_(seen_news)
            ).group_by(
                News.news_id, News.category, News.topic, News.headline
            ).order_by(
                func.avg(ExposureLog.clicked * 1.0).desc(),
                func.count(func.distinct(ExposureLog.impression_id)).desc()
            ).limit(limit).all()
            
            return [{
                'news_id': result.news_id,
                'category': result.category,
                'topic': result.topic,
                'headline': result.headline,
                'popularity': result.popularity,
                'predicted_interest': float(result.avg_click_rate) if result.avg_click_rate else 0
            } for result in query]
        
        return self.execute_timed_query(
            'user_recommendations', _query,
            user_id=user_id, limit=limit, reference_date=reference_date
        )
    
    def get_query_performance_stats(self, hours: int = 24) -> List[Dict]:
        """Get query performance statistics for monitoring"""
        def _query(hours):
            cutoff = datetime.now() - timedelta(hours=hours)
            
            results = self.session.query(
                QueryLog.query_type,
                func.count(QueryLog.id).label('total_queries'),
                func.avg(QueryLog.execution_time).label('avg_execution_time'),
                func.max(QueryLog.execution_time).label('max_execution_time'),
                func.min(QueryLog.execution_time).label('min_execution_time'),
                func.sum(func.cast(QueryLog.success == False, Integer)).label('failed_queries')
            ).filter(
                QueryLog.timestamp >= cutoff
            ).group_by(QueryLog.query_type).all()
            
            return [{
                'query_type': result.query_type,
                'total_queries': result.total_queries,
                'avg_execution_time': float(result.avg_execution_time) if result.avg_execution_time else 0,
                'max_execution_time': float(result.max_execution_time) if result.max_execution_time else 0,
                'min_execution_time': float(result.min_execution_time) if result.min_execution_time else 0,
                'failed_queries': result.failed_queries or 0,
                'success_rate': ((result.total_queries - (result.failed_queries or 0)) / max(result.total_queries, 1)) * 100
            } for result in results]
        
        return self.execute_timed_query(
            'performance_stats', _query, hours=hours
        )
    
    def close(self):
        """Close the database session"""
        try:
            self.session.close()
            self.logger.info("Database session closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing database session: {e}")
            raise
            
    def __del__(self):
        """Destructor to ensure resources are cleaned up"""
        try:
            self.close()
        except:
            pass
