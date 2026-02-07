from pyspark.sql import SparkSession
import requests
import json
import pandas as pd
import datetime
from google.cloud import storage

# Initialize Spark Session
spark = SparkSession.builder.appName("CustomerReviewsAPI").getOrCreate()

# API EndPoint
API_URL = "https://6975e498c0c36a2a994fb2f3.mockapi.io/retailer_projects/reviews"

# Step 1: Fetch data from API
response = requests.get(API_URL)

if response.status_code == 200:
    data = response.json()
    print(f"✅ Successfully fetched {len(data)} records.")
else:
    print(f"❌ Failed to fetch data. Status code : {response.status_code}")
    exit()
    
# Step 2: Convert API Data to Pandas DataFrame
df_pandas = pd.DataFrame(data)

# Step 3: Get Current Date from File Naming
today = datetime.date.today().strftime('%Y%m%d') # Format: YYYYMMDD

# Step 4: Define File Paths with Date
local_parquet_file = f"/tmp/customer_reviews_{today}.parquet"
GCS_BUCKET = "retailer-datalake-project-01-02-2026"
GCS_PATH = f"landing/customer_reviews/customer_reviews_{today}.parquet"

# Step 5: Save Pandas DataFrame as Parquet Locally
df_pandas.to_parquet(local_parquet_file, index=False)

# Step 6: Upload  Parquet File to GCS
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET)
blob = bucket.blob(GCS_PATH)
blob.upload_from_filename(local_parquet_file)


print(f"✅ Data Successfully written to gs://{GCS_BUCKET}/{GCS_PATH}")
