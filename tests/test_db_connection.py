from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the DATABASE_URL from the environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

# Initialize SQLAlchemy engine
engine = create_engine(DATABASE_URL)

def test_database_connection():
    """
    Test the connection to the database.

    Prints a success message if the connection is established.
    Prints an error message if the connection fails.
    """
    try:
        # Attempt to connect to the database
        with engine.connect() as connection:
            print("Database connection successful!")

            # Run a test query to ensure proper connection
            result = connection.execute(text("SELECT 1")).scalar()
            print(f"Test query result: {result}")
    except OperationalError as e:
        print(f"Database connection failed: {e}")
    finally:
        # Dispose of the engine to close all connections
        engine.dispose()

if __name__ == "__main__":
    test_database_connection()
