from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
from sqlalchemy import inspect

# Load environment variables from .env file
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

# Initialize SQLAlchemy
engine = create_engine(DATABASE_URL)
Base = declarative_base()

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


# Function to initialize the database
def init_db():
    from server.models import WeatherData, BatchMetadata
    Base.metadata.create_all(bind=engine)  # Create tables
    
    
if __name__ == "__main__":
    init_db()
