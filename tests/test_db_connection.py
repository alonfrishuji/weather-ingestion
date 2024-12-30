from sqlalchemy import create_engine, inspect
import os
from dotenv import load_dotenv

load_dotenv()

# Use the External Database URL
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
inspector = inspect(engine)

print("Tables in the database:")
for table_name in inspector.get_table_names():
    print(table_name)
