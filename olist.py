import pandas as pd
from sqlalchemy import create_engine, types, text
import os
import matplotlib.pyplot as plt

# Connection parameters
user = 'root' # Update with your MySQL username
password = 'root' # Update with your MySQL password
host = 'localhost' # Update if your MySQL server is not local
port = '3306' # Update if your MySQL server uses a different port
db = 'olist_intelligence' # Update with your database name
# Create the connection URL
url = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}'
# Create the SQLAlchemy engine
engine = create_engine(url)
# We define specific types to ensure IDs are VARCHAR and Prices are Decimal
table_schemas = {
    'customers': {
        'customer_id': types.VARCHAR(50),
        'customer_unique_id': types.VARCHAR(50),
        'customer_zip_code_prefix': types.VARCHAR(10)
    },
    'order_items': {
        'order_id': types.VARCHAR(50),
        'order_item_id': types.INTEGER(),
        'product_id': types.VARCHAR(50),
        'seller_id': types.VARCHAR(50),
        'price': types.DECIMAL(10, 2),
        'freight_value': types.DECIMAL(10, 2)
    },
    'orders': {
        'order_id': types.VARCHAR(50),
        'customer_id': types.VARCHAR(50),
    },
    'products': {
        'product_id': types.VARCHAR(50),
    },
}
# csv_file = 'olist_orders_dataset.csv'
# "Kaggle_File_Name.csv" : "SQL_Table_Name"
files_to_upload = {
    'olist_customers_dataset.csv': 'customers',
    'olist_geolocation_dataset.csv': 'geolocation',
    'olist_order_items_dataset.csv': 'order_items',
    'olist_order_payments_dataset.csv': 'order_payments',
    'olist_order_reviews_dataset.csv': 'order_reviews',
    'olist_orders_dataset.csv': 'orders',
    'olist_products_dataset.csv': 'products',
    'olist_sellers_dataset.csv': 'sellers',
    'product_category_name_translation.csv': 'product_category_translation'
}
# THE ETL Engine
print("Starting ETL Process...")
for file_name, table_name in files_to_upload.items():
    if os.path.exists(file_name):
        print(f"Uploading {file_name} to {table_name}...")
        df = pd.read_csv(file_name)
        target_schemas = table_schemas.get(table_name, None)

        df.to_sql(
            table_name, 
            con=engine, 
            if_exists='replace', 
            index=False,
            chunksize=10000,
            dtype=table_schemas.get(table_name) # Use defined schema if available
        )
        #integrity check
        with engine.connect() as conn:
                db_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()            
                if len(df) != db_count:
                    print(f"Warning: Row count mismatch for {table_name}. CSV has {len(df)} rows, but database has {db_count} rows.")
    else: 
        print(f"File {file_name} not found. Skipping upload for {table_name}.")
print(f"{file_name} uploaded successfully.")
print("Starting data upload...")
for file_name, table_name in files_to_upload.items():
    if os.path.exists(file_name):
        print(f"Uploading {file_name} to {table_name}...")
        df = pd.read_csv(file_name)
# 10000 rows at a time makes it much easier on the MySQL buffer
        df.to_sql(
            table_name, 
            con=engine, 
            if_exists='replace', 
            index=False, 
            chunksize=10000 
        )        
        print(f"{file_name} uploaded successfully.")
    else:
        print(f"File {file_name} not found. Skipping upload for {table_name}.")

print("Data upload completed.")

print("Data Cleaning---------")
df_orders = pd.read_sql('SELECT * FROM orders', con=engine)
df_items = pd.read_sql('SELECT * FROM order_items', con=engine)
df_reviews = pd.read_sql('SELECT * FROM order_reviews', con=engine)
df_customers = pd.read_sql('SELECT * FROM customers', con=engine)

#convert timestamp
date_cols = ['order_purchase_timestamp', 
            'order_approved_at',
            'order_delivered_carrier_date', 
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
            ]
for col in date_cols:
    df_orders[col] = pd.to_datetime(df_orders[col], errors='coerce')

# check for price anomalies
df_items = df_items[df_items['price'] > 0]

#zip code anomalies
df_customers['customer_zip_code_prefix'] = df_customers['customer_zip_code_prefix'].astype(str).str.zfill(5)
print("Data Cleaning Completed.")

#Visualization
print("Starting Visualization...")
df_orders['Month_Year'] = df_orders['order_purchase_timestamp'].dt.to_period('M')
order_counts = df_orders.groupby('Month_Year').size()

plt.figure(figsize=(12, 5))
order_counts.plot(kind='line', marker='o', color='#2ecc71')
plt.title('Monthly Order Volume')
plt.grid(True, alpha=0.3)
plt.show()

# Calculate the difference in days
df_orders['actual_days'] = (df_orders['order_delivered_customer_date'] - df_orders['order_purchase_timestamp']).dt.days
df_orders['estimated_days'] = (df_orders['order_estimated_delivery_date'] - df_orders['order_purchase_timestamp']).dt.days

# Drop NaNs for the plot (rows where delivery hasn't happened)
delivery_clean = df_orders[['actual_days', 'estimated_days']].dropna()

plt.figure(figsize=(8, 6))
plt.boxplot([delivery_clean['actual_days'], delivery_clean['estimated_days']], 
            tick_labels=['Actual', 'Estimated'], patch_artist=True)
plt.title('Delivery Performance (Days)')
plt.ylabel('Days')
plt.ylim(0, 50) # Setting limit to see the "bulk" of data clearly
plt.show()

# --- VIZ 3: Review Scores (Bar Chart) ---
review_dist = df_reviews['review_score'].value_counts().sort_index()

plt.figure(figsize=(8, 5))
review_dist.plot(kind='bar', color='skyblue', edgecolor='black')
plt.title('Distribution of Review Scores')
plt.xlabel('Star Rating')
plt.ylabel('Count')
plt.xticks(rotation=0)
plt.show()

print("--- Script Finished ---")