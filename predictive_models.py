# predictive_models.py
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import json
from datetime import datetime, timedelta
import logging

# Model-specific imports
from prophet import Prophet
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def main():
    logger.info("Starting predictive model training...")
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    engine = create_engine(database_url)

    # --------------------------------------------------------------------
    # 1. SALES FORECASTING with Prophet
    # --------------------------------------------------------------------
    logger.info("Training Prophet model for sales forecasting...")
    
    # Load and prepare sales data
    sales_df = pd.read_sql("SELECT date, sales_amount FROM sales_data ORDER BY date", engine)
    sales_df = sales_df.rename(columns={'date': 'ds', 'sales_amount': 'y'})
    
    # Initialize and fit the Prophet model
    model_prophet = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False
    )
    model_prophet.fit(sales_df)
    
    # Create future dataframe for the next 90 days and forecast
    future = model_prophet.make_future_dataframe(periods=90)
    forecast = model_prophet.predict(future)
    
    # Save the forecast results to the database for the dashboard
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_sql('sales_forecast', engine, if_exists='replace', index=False)
    logger.info("Sales forecast saved to 'sales_forecast' table.")
    
    # --------------------------------------------------------------------
    # 2. CHURN PREDICTION with XGBoost
    # --------------------------------------------------------------------
    logger.info("Training XGBoost model for churn prediction...")
    
    # Load and prepare customer data for ML
    query = """
        SELECT 
            tenure, 
            "MonthlyCharges", 
            "TotalCharges",
            "Contract", 
            "InternetService",
            "OnlineSecurity",
            "TechSupport",
            "PaymentMethod",
            "Churn"
        FROM telco_churn
    """
    churn_df = pd.read_sql(query, engine)
    
    # Preprocess categorical variables using Label Encoding
    label_encoders = {}
    categorical_cols = ["Contract", "InternetService", "OnlineSecurity", "TechSupport", "PaymentMethod"]
    
    for col in categorical_cols:
        le = LabelEncoder()
        churn_df[col] = le.fit_transform(churn_df[col].astype(str))
        label_encoders[col] = le
    
    # Convert target variable 'Churn'
    churn_df['Churn'] = churn_df['Churn'].apply(lambda x: 1 if x == 'Yes' else 0)
    
    # Define features (X) and target (y)
    X = churn_df.drop('Churn', axis=1)
    y = churn_df['Churn']
    
    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # Initialize and train the XGBoost model
    model_xgb = xgb.XGBClassifier(
        objective='binary:logistic',
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        use_label_encoder=False
    )
    model_xgb.fit(X_train, y_train)
    
    # Make predictions and evaluate the model
    y_pred = model_xgb.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logger.info(f"XGBoost Model Accuracy: {accuracy:.4f}")
    logger.info("\nClassification Report:\n" + classification_report(y_test, y_pred))
    
    # Save the trained model to a file for later use
    model_xgb.save_model('xgboost_churn_model.json')
    
    # Save the label encoders for use in the dashboard/API
    with open('label_encoders.json', 'w') as f:
        encoder_data = {col: list(le.classes_) for col, le in label_encoders.items()}
        json.dump(encoder_data, f)
    
    logger.info("Churn prediction model saved as 'xgboost_churn_model.json'")
    logger.info("Label encoders saved as 'label_encoders.json'")
    logger.info("Predictive model training completed successfully!")

if __name__ == "__main__":
    main()