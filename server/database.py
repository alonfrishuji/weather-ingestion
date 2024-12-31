import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Load environment variables from .env file
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

engine = create_engine(
    DATABASE_URL,
    pool_size=30,          # Base connections to keep open
    max_overflow=20,       # Additional connections beyond pool_size
    pool_timeout=40,       # Timeout for acquiring a connection
    pool_recycle=28000,    # Recycle connections slightly before server timeout
    pool_pre_ping=True,    # Check if the connection is alive before using
)
Base = declarative_base()

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


# Function to initialize the database
def init_db():
    print("Starting database initialization...")
    from server.models import BatchMetadata, WeatherData
    try:
        Base.metadata.create_all(bind=engine)
        print("Tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")

    print("Database initialization complete!")
    
    
if __name__ == "__main__":
    init_db()
