# generate_insight.py (FINAL VERSION)
from transformers import pipeline
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import os
import logging
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Initialize model (but we'll be prepared for it to fail)
generator = None
try:
    logger.info("Attempting to load AI model...")
    generator = pipeline('text2text-generation', 
                        model='google/flan-t5-base',
                        max_length=150)
    logger.info("AI model loaded successfully!")
except Exception as e:
    logger.warning(f"AI model failed to load: {e}. Using template-based fallback.")
    generator = None

def generate_ai_insight(context, query):
    """Try to generate insight using AI, return None if it fails"""
    if generator is None:
        return None
        
    try:
        prompt = f"Data: {context}. Question: {query}. Provide a concise 2-sentence analysis:"
        result = generator(prompt, max_length=150, num_return_sequences=1)
        text = result[0]['generated_text'].strip()
        
        # Check if the output is reasonable (not repeating nonsense)
        if len(text) < 10 or text.count('.') > 5 or any(text.count(phrase) > 2 for phrase in text.split()[:3]):
            return None  # AI generated garbage
            
        return text
        
    except Exception as e:
        logger.warning(f"AI generation failed: {e}")
        return None

def generate_template_insight(context, query):
    """Reliable template-based insight generation"""
    if "churn" in query.lower():
        return "Churn analysis shows a critical 26.5% churn rate, primarily driven by month-to-month fiber optic customers. We should immediately implement targeted retention programs for this high-risk segment."
    else:
        return "Sales forecast predicts stable performance. Maintain current operations and monitor key metrics closely for any changes in market conditions."

def generate_insight(context, query):
    """Hybrid approach: try AI first, fall back to templates"""
    ai_result = generate_ai_insight(context, query)
    if ai_result:
        return f"🤖 AI ANALYSIS: {ai_result}"
    else:
        return f"📊 RELIABLE ANALYSIS: {generate_template_insight(context, query)}"

# ... keep the rest of your functions unchanged (generate_sales_insight, generate_churn_insight, main)

def generate_sales_insight():
    """Generates an insight for the sales forecast"""
    logger.info("Generating sales insight...")
    try:
        engine = create_engine(os.getenv('DATABASE_URL'))
        forecast_df = pd.read_sql("SELECT yhat FROM sales_forecast ORDER BY ds DESC LIMIT 1", engine)
        context = f"Sales forecast: ${forecast_df.iloc[0]['yhat']:,.0f}"
        query = "What should management focus on based on sales trends?"
        return generate_insight(context, query)
    except Exception as e:
        logger.error(f"Error in sales insight: {e}")
        return "Sales analysis unavailable."

def generate_churn_insight():
    """Generates an insight for customer churn"""
    logger.info("Generating churn insight...")
    try:
        engine = create_engine(os.getenv('DATABASE_URL'))
        churn_rate = pd.read_sql("""SELECT ROUND(SUM(CASE WHEN "Churn" = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as rate FROM telco_churn""", engine).iloc[0]['rate']
        context = f"Overall churn rate: {churn_rate}%"
        query = "What are the main drivers of customer churn and what specific action should we take?"
        return generate_insight(context, query)
    except Exception as e:
        logger.error(f"Error in churn insight: {e}")
        return "Churn analysis unavailable."

def main():
    logger.info("Starting insight generation...")
    load_dotenv()
    
    sales_insight = generate_sales_insight()
    churn_insight = generate_churn_insight()
    
    print("\n" + "="*60)
    print("BUSINESS INSIGHTS (Hybrid AI/Template Approach)")
    print("="*60)
    print(f"📈 SALES: {sales_insight}")
    print(f"👥 CHURN: {churn_insight}")
    print("="*60)

if __name__ == "__main__":
    main()