# load_stock_data.py
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import re

# 1. Load database credentials from .env file
load_dotenv()
database_url = os.getenv('DATABASE_URL')
engine = create_engine(database_url)

# 2. Define the stock ticker and period
ticker = "VZ"  # Verizon stock ticker
start_date = "2010-01-01"
end_date = "2024-12-31"

# 3. Download historical stock data
stock_data = yf.download(ticker, start=start_date, end=end_date)

# 4. Reset index to make 'Date' a column and clean up the data
stock_data.reset_index(inplace=True)

# 4.1 FIXED: CLEAN THE COLUMN NAMES (Handle tuples)
# Extract the first element of each tuple (the actual column name)
# Then clean any special characters and convert to lowercase
clean_columns = []
for col in stock_data.columns:
    if isinstance(col, tuple):
        # Take the first part of the tuple (e.g., 'Date' from ('Date', ''))
        col_name = str(col[0])
    else:
        col_name = str(col)
    
    # Remove any non-alphanumeric characters except underscores
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '', col_name).lower()
    clean_columns.append(clean_name)

stock_data.columns = clean_columns
stock_data['ticker'] = ticker  # Add a column to identify the stock

# Show the first few rows
print("Verizon (VZ) stock data downloaded successfully!")
print(stock_data.head())
print("\nColumns:", stock_data.columns.tolist())

# 5. Load the data into a new table in PostgreSQL
stock_data.to_sql('stock_data', engine, if_exists='replace', index=False)
print("Stock data successfully loaded into the 'stock_data' table in PostgreSQL!")