# train_models.py
import pandas as pd
import numpy as np
from faker import Faker
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import logging

# Set up logging to see all output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def main():
    try:
        logger.info("Script started successfully")
        
        # --- 1. Load Environment and Connect to DB ---
        logger.info("Loading environment variables")
        load_dotenv()
        database_url = os.getenv('DATABASE_URL')
        
        if not database_url:
            logger.error("DATABASE_URL not found in .env file")
            return
        
        logger.info(f"Database URL loaded: {database_url}")
        engine = create_engine(database_url)
        
        # Test connection
        with engine.connect() as conn:
            logger.info("Database connection test successful")

        # --- 2. Generate Synthetic Sales Data ---
        logger.info("Generating synthetic sales data")
        fake = Faker()
        np.random.seed(42)  # For reproducible results

        # Create a date range for the last 3 years
        dates = pd.date_range(end=datetime.today(), periods=1095, freq='D') # 3 years of days

        # Generate synthetic sales data
        sales_records = []
        for date in dates:
            # Simulate higher sales on weekdays and in certain months
            is_weekday = date.weekday() < 5
            month_factor = 1.2 if date.month in [11, 12] else 1.0  # Holiday season boost
            
            base_sales = np.random.normal(50000, 15000)  # Base daily sales
            daily_sales = base_sales * (1.1 if is_weekday else 0.7) * month_factor
            
            sales_records.append({
                'date': date,
                'sales_amount': max(daily_sales, 10000),  # Ensure not negative
                'units_sold': int(max(daily_sales / 100, 100))  # Mock unit count
            })

        sales_df = pd.DataFrame(sales_records)
        logger.info(f"Sales data generated: {len(sales_df)} rows")

        # --- 3. Generate REALISTIC Synthetic Customer Activity Data ---
        logger.info("Reading customer data from database")
        # Read the existing customer list AND their tenure, contract, and services
        existing_customers_df = pd.read_sql("""
            SELECT 
                "customerID", 
                tenure, 
                "Contract",
                "InternetService",
                "OnlineSecurity",
                "TechSupport",
                "Churn"
            FROM telco_churn
        """, engine)
        
        logger.info(f"Loaded {len(existing_customers_df)} customers from telco_churn table")

        activity_records = []
        total_logins_estimated = 0
        total_tickets_estimated = 0

        for index, customer in existing_customers_df.iterrows():
            customer_id = customer['customerID']
            tenure_months = customer['tenure']
            
            # --- Rule 1: Login Frequency based on Tenure & Engagement ---
            # Base logins: 10-30 times per month. More engaged customers login more.
            base_logins_per_month = np.random.randint(10, 30)
            
            # Long-term customers might be slightly less active recently
            if tenure_months > 48:
                engagement_factor = 0.8  # 20% less active
            else:
                engagement_factor = 1.0
                
            # Customers with add-ons (like TechSupport) are more engaged
            if customer['TechSupport'] == 'Yes':
                engagement_factor *= 1.2
                
            customer_logins = int(base_logins_per_month * tenure_months * engagement_factor)
            total_logins_estimated += customer_logins
            
            # --- Rule 2: Support Ticket Probability based on Churn Risk ---
            # High-risk factors: Fiber optic, no online security, month-to-month contract
            is_high_risk = (
                customer['InternetService'] == 'Fiber optic' and
                customer['OnlineSecurity'] == 'No' and
                customer['Contract'] == 'Month-to-month'
            )
            
            # Base ticket probability is higher for high-risk customers
            base_ticket_probability = 0.4 if is_high_risk else 0.1
            
            # Generate activity for each login
            for i in range(customer_logins):
                # Simulate a login date spread throughout the customer's tenure
                login_date = fake.date_between(
                    start_date=datetime.today() - timedelta(days=tenure_months*30),
                    end_date='today'
                )
                
                # Decide if this login resulted in a support ticket
                raised_ticket = 1 if np.random.random() < base_ticket_probability else 0
                total_tickets_estimated += raised_ticket
                
                activity_records.append({
                    'customer_id': customer_id,
                    'date': login_date,
                    'login_count': 1,
                    'support_tickets_raised': raised_ticket
                })

        activity_df = pd.DataFrame(activity_records)
        logger.info(f"Activity data generated: {len(activity_df)} total logins")
        logger.info(f"Average logins per customer: {len(activity_df) / len(existing_customers_df):.2f}")
        logger.info(f"Total support tickets estimated: {total_tickets_estimated}")
        logger.info(f"First 5 activity records: \n{activity_df.head()}")

        # --- 4. Load Synthetic Data into PostgreSQL ---
        logger.info("Loading data into PostgreSQL database")
        
        # Load sales data
        sales_df.to_sql('sales_data', engine, if_exists='replace', index=False)
        logger.info("Sales data loaded into 'sales_data' table")
        
        # Load activity data
        activity_df.to_sql('customer_activity', engine, if_exists='replace', index=False)
        logger.info("Activity data loaded into 'customer_activity' table")

        logger.info("Synthetic sales and customer activity data generated and loaded successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()