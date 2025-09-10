# load_data.py
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv  # Import the library to read .env

# Load environment variables from the .env file
load_dotenv()

# 1. EXTRACT: READ THE CSV FILE
file_path = 'WA_Fn-UseC_-Telco-Customer-Churn.csv'
df = pd.read_csv(file_path)
print("CSV file loaded successfully!")

# 2. TRANSFORM: CLEAN THE DATA
df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
df['TotalCharges'].fillna(0, inplace=True)
print("'TotalCharges' column cleaned.")

# 3. LOAD: CONNECT TO THE DATABASE AND UPLOAD THE DATA
# Get the connection string from the environment variable
database_url = os.getenv('DATABASE_URL')  # This reads the DATABASE_URL from .env

# Check if the variable was loaded correctly
if not database_url:
    raise ValueError("ERROR: DATABASE_URL not found in .env file. Please check your setup.")

# Create the engine object that knows how to talk to your database
engine = create_engine(database_url)

# 4. UPLOAD THE CLEANED DATA TO THE SQL TABLE
df.to_sql('telco_churn', engine, if_exists='replace', index=False)
print("Data successfully loaded into the 'telco_churn' table in PostgreSQL!")