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
engine = create_engine(DATABASE_URL, echo=True)
Base = declarative_base()

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


# Function to initialize the database
def init_db():
    print("Starting database initialization...")
    from server.models import WeatherData, BatchMetadata
    try:
        Base.metadata.create_all(bind=engine)
        print("Tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")

    print("Database initialization complete!")
    
    
if __name__ == "__main__":
    init_db()
