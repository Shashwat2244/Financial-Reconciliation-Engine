import csv
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()

def generate_data(num_rows=50000, output_dir="."):
    orders = []
    settlements = []
    
    print(f"Generating {num_rows} rows of mock data...")
    for i in range(num_rows):
        order_id = f"ORD-{fake.uuid4()[:8]}"
        user_id = f"USR-{fake.random_int(min=1000, max=9999)}"
        amount = round(random.uniform(10.0, 500.0), 2)
        
        # Order time
        timestamp_utc = fake.date_time_between(start_date='-30d', end_date='now')
        
        orders.append([order_id, user_id, amount, timestamp_utc.strftime('%Y-%m-%dT%H:%M:%SZ')])
        
        # Anomaly generation (5%)
        is_anomaly = random.random() < 0.05
        
        settlement_id = f"SET-{fake.uuid4()[:8]}"
        settled_amount = amount
        fee = round(amount * 0.02, 2) # 2% fee
        
        # Settlement time (EST) - normally 1-2 hours after
        settlement_time = timestamp_utc + timedelta(hours=random.randint(1, 2))
        
        if is_anomaly:
            anomaly_type = random.choice(['delayed', 'missing_order_id', 'decimal_mismatch'])
            if anomaly_type == 'delayed':
                settlement_time = timestamp_utc + timedelta(days=random.randint(3, 7))
            elif anomaly_type == 'missing_order_id':
                order_id = "" # blank or unknown
            elif anomaly_type == 'decimal_mismatch':
                settled_amount = round(amount - 0.01, 2)
        
        settlements.append([settlement_id, order_id, settled_amount, fee, settlement_time.strftime('%Y-%m-%dT%H:%M:%S')])
        
    orders_path = os.path.join(output_dir, 'internal_orders.csv')
    settlements_path = os.path.join(output_dir, 'gateway_settlements.csv')

    with open(orders_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['order_id', 'user_id', 'amount', 'timestamp_utc'])
        writer.writerows(orders)
        
    with open(settlements_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['settlement_id', 'order_id', 'settled_amount', 'fee', 'timestamp_est'])
        writer.writerows(settlements)
        
    print(f"Done. Files saved to {orders_path} and {settlements_path}")

if __name__ == '__main__':
    # Use absolute path resolving for robustness
    script_dir = os.path.dirname(os.path.abspath(__file__))
    generate_data(num_rows=50000, output_dir=script_dir)
