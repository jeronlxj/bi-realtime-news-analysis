from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "postgresql://newsuser:newpass@postgres:5432/newsdb"

Base = declarative_base()

class NewsTopic(Base):
    __tablename__ = 'news_topics'
    
    id = Column(Integer, Sequence('topic_id_seq'), primary_key=True)
    title = Column(String(255), nullable=False)
    content = Column(String, nullable=False)
    timestamp = Column(Integer, nullable=False)

engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

def get_session():
    return Session()

def add_news_topic(title, content, timestamp):
    session = get_session()
    new_topic = NewsTopic(title=title, content=content, timestamp=timestamp)
    session.add(new_topic)
    session.commit()
    session.close()

def get_all_news_topics():
    session = get_session()
    topics = session.query(NewsTopic).all()
    session.close()
    return topics

def get_news_topic_by_id(topic_id):
    session = get_session()
    topic = session.query(NewsTopic).filter(NewsTopic.id == topic_id).first()
    session.close()
    return topic