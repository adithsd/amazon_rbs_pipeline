"""
Module: data_generator.py
Author: Senior Data Engineering Mentor (Amazon RBS Portfolio Project)
Description: Generates a high-volume e-commerce catalog transaction dataset 
             with intentional data anomalies to simulate real-world RBS upstream defects.
Total Records: 500,000
"""

import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

# Initialize Faker for realistic merchant/product names
fake = Faker()
random.seed(42)  # For reproducibility

# Configuration
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '../data/ingress')
TOTAL_RECORDS = 500000
BATCH_SIZE = 100000  # Memory-efficient chunking to prevent system OOM

def generate_batch(batch_size: int, anomalies_pct: float = 0.05) -> pd.DataFrame:
    """
    Generates a localized batch of transaction records with embedded anomalies.
    """
    data = []
    current_time = datetime.now()
    
    # Pre-generate standard categories and regions for consistency
    categories = ['Electronics', 'Apparel', 'Home & Kitchen', 'Automotive', 'Books', 'Beauty']
    regions = ['US-EAST', 'US-WEST', 'EU-WEST', 'APAC-SOUTH', 'APAC-EAST']

    for _ in range(batch_size):
        # Base clean record structure
        record = {
            "transaction_id": str(uuid.uuid4()),
            "product_id": f"PRD{random.randint(100000, 999999)}",
            "merchant_id": f"MERCH{random.randint(1000, 9999)}",
            "category": random.choice(categories),
            "marketplace_region": random.choice(regions),
            "price": round(random.uniform(5.99, 1499.99), 2),
            "quantity": random.randint(1, 50),
            "transaction_date": (current_time - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S'),
            "ingest_timestamp": current_time.strftime('%Y-%m-%d %H:%M:%S')
        }

        # Inject Anomalies based on target thresholds (~5% of data corrupted)
        if random.random() < anomalies_pct:
            anomaly_type = random.choice(['NULL_PRODUCT', 'STRING_PRICE', 'NEG_QTY', 'FUTURE_DATE'])
            
            if anomaly_type == 'NULL_PRODUCT':
                # Critical structural defect: Amazon catalog requires non-null product identifiers
                record['product_id'] = None
                
            elif anomaly_type == 'STRING_PRICE':
                # Type mismatch / Schema drift: simulating malformed upstream API payloads
                record['price'] = "ERROR_MALFORMED_VALUE"
                
            elif anomaly_type == 'NEG_QTY':
                # Business logic defect: Negative inventory quantities disrupt fulfillment tracking
                record['quantity'] = random.randint(-10, -1)
                
            elif anomaly_type == 'FUTURE_DATE':
                # Temporal validation defect: Events cannot occur ahead of execution processing time
                record['transaction_date'] = (current_time + timedelta(days=random.randint(365, 1000))).strftime('%Y-%m-%d %H:%M:%S')

        data.append(record)
        
    return pd.DataFrame(data)

def main():
    print(f"🚀 Starting ingestion simulation layer execution at: {datetime.now()}")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    target_file = os.path.join(OUTPUT_DIR, f"catalog_transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    
    # Process and stream batches to file to prevent local OOM (Out of Memory) issues
    is_first_batch = True
    processed_records = 0
    
    while processed_records < TOTAL_RECORDS:
        df_batch = generate_batch(batch_size=BATCH_SIZE)
        
        if is_first_batch:
            df_batch.to_csv(target_file, index=False, mode='w')
            is_first_batch = False
        else:
            df_batch.to_csv(target_file, index=False, mode='a', header=False)
            
        processed_records += BATCH_SIZE
        print(f"📦 Successfully flushed {processed_records}/{TOTAL_RECORDS} records to disk.")

    print(f"✅ Ingestion baseline complete. Dataset dropped at: {target_file}")

if __name__ == "__main__":
    main()