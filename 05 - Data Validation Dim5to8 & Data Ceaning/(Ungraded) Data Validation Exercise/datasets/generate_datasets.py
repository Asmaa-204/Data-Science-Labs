import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)

# ── DATASET 1: E-Commerce Orders ─────────────────────────────────────────────
n1 = 3000
order_ids = range(1001, 1001 + n1)
categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']
statuses = ['pending', 'shipped', 'delivered', 'cancelled', 'returned']
countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Australia']

start_date = datetime(2023, 1, 1)
order_dates = [
    start_date + timedelta(days=int(x)) for x in np.random.uniform(0, 365, n1)
]
order_timestamps = [d.strftime('%Y-%m-%d %H:%M:%S') for d in order_dates]

df_orders = pd.DataFrame(
    {
        'order_id': list(order_ids),
        'customer_id': np.random.randint(1, 800, n1),
        'order_timestamp': order_timestamps,
        'category': np.random.choice(categories, n1),
        'quantity': np.random.randint(1, 10, n1),
        'unit_price': np.round(np.random.uniform(5, 500, n1), 2),
        'discount_pct': np.round(np.random.uniform(0, 0.4, n1), 3),
        'status': np.random.choice(statuses, n1),
        'country': np.random.choice(countries, n1),
        'rating': np.round(np.random.uniform(1.0, 5.0, n1), 1),
    }
)

# Inject realistic errors
df_orders.loc[10, 'order_id'] = 1001  # duplicate
df_orders.loc[25, 'unit_price'] = -50.0  # negative price
df_orders.loc[50, 'discount_pct'] = 1.8  # >100% discount
df_orders.loc[75, 'rating'] = 7.5  # out of range
df_orders.loc[100, 'status'] = 'unknown_status'  # invalid category
df_orders.loc[120, 'country'] = 'Mars'  # invalid country
df_orders.loc[200:205, 'customer_id'] = np.nan  # missing values
df_orders.loc[300:302, 'category'] = np.nan
df_orders.loc[500, 'quantity'] = 9999  # outlier

df_orders.to_csv('ecommerce_orders.csv', index=False)
print(f"ecommerce_orders.csv: {len(df_orders)} rows, {len(df_orders.columns)} cols")

# ── DATASET 2: Patient Health Records ────────────────────────────────────────
n2 = 2000
genders = ['Male', 'Female']
blood_types = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']
departments = ['Cardiology', 'Neurology', 'Orthopedics', 'Oncology', 'Pediatrics']

admit_dates = [
    datetime(2022, 1, 1) + timedelta(days=int(x)) for x in np.random.uniform(0, 730, n2)
]
admit_timestamps = [d.strftime('%Y-%m-%d %H:%M:%S') for d in admit_dates]

ages = np.random.randint(18, 90, n2)
heights_cm = np.round(np.random.normal(170, 12, n2), 1)
weights_kg = np.round(np.random.normal(75, 15, n2), 1)
bp_systolic = np.random.randint(90, 180, n2)
bp_diastolic = np.random.randint(60, 120, n2)

df_health = pd.DataFrame(
    {
        'patient_id': range(5001, 5001 + n2),
        'age': ages,
        'gender': np.random.choice(genders, n2),
        'blood_type': np.random.choice(blood_types, n2),
        'admission_date': admit_timestamps,
        'department': np.random.choice(departments, n2),
        'height_cm': heights_cm,
        'weight_kg': weights_kg,
        'bp_systolic': bp_systolic,
        'bp_diastolic': bp_diastolic,
    }
)

# Inject realistic errors
df_health.loc[5, 'patient_id'] = 5001  # duplicate
df_health.loc[20, 'age'] = -3  # invalid age
df_health.loc[40, 'age'] = 200  # unrealistic age
df_health.loc[60, 'height_cm'] = 350.0  # outlier height
df_health.loc[80, 'weight_kg'] = -10.0  # negative weight
df_health.loc[100, 'gender'] = 'Unknown'  # invalid category
df_health.loc[150, 'blood_type'] = 'XY'  # invalid blood type
df_health.loc[200:210, 'bp_systolic'] = np.nan  # missing values
df_health.loc[400:405, 'department'] = np.nan
df_health.loc[800, 'bp_systolic'] = 300  # extreme outlier

df_health.to_csv('patient_health_records.csv', index=False)
print(
    f"patient_health_records.csv: {len(df_health)} rows, {len(df_health.columns)} cols"
)
