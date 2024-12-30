from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

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
    print("Starting database initialization...")
    from server.models import WeatherData, BatchMetadata
    print("Models imported:", WeatherData, BatchMetadata)
    print("Metadata registered tables:", Base.metadata.tables.keys())
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully!")
    
    
if __name__ == "__main__":
    init_db()
