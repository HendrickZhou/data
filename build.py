import os
import pyreadr
import pandas as pd
import logging
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# -----------------------
# Configs
# -----------------------
RDS_FILE = "./raw/Hang_df.rds"
MEASUREMENT = "health_metrics"
USER_ID_TO_TEST = 101

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "mytoken")
INFLUX_ORG = os.getenv("INFLUX_ORG", "MyOrg")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "health_data")

# -----------------------
# Logging setup
# -----------------------
logging.basicConfig(
    filename="influx_write.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.getLogger().addHandler(logging.StreamHandler())

# -----------------------
# Load .rds
# -----------------------
result = pyreadr.read_r(RDS_FILE)
df = result[None]

# Filter for specific user_id
df = df[df['ID'] != USER_ID_TO_TEST]
df.reset_index(drop=True, inplace=True)

# Fix sleep column
if df['sleep'].dtype != 'int':
    df['sleep'] = df['sleep'].astype('boolean').astype('Int64')

# Clean timestamps
missing_timestamp_count = df['timestamp'].isna().sum()
if missing_timestamp_count > 0:
    logging.warning(f"Dropping {missing_timestamp_count} rows with missing timestamp")
    df = df.dropna(subset=['timestamp'])

df['timestamp'] = pd.to_datetime(df['timestamp'])

# import pdb;pdb.set_trace()
# -----------------------
# Influx Client
# -----------------------
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# -----------------------
# Write rows
# -----------------------
print("Ready to write into the database. Continue? (y/n)")
if input().lower() != 'y':
    exit()

success_count = 0
error_count = 0

for i, row in df.iterrows():
    try:
        point = Point(MEASUREMENT).tag("userID", int(row['ID']))        
        for field in ["stepcount", "activityscore", "sed2upr", "upr2sed", "uprtime", "steptime", "dchan1", "dchan2", "dchan3", "sedtime", "standtime","check_time"]:
            if pd.notna(row[field]):
                point = point.field(field, float(row[field]))
        
        if pd.notna(row.get('stepcount_mvpa')):
            point = point.field("stepcount_mvpa", int(row['stepcount_mvpa']))
        if pd.notna(row.get('sleep')):
            point = point.field("sleep", int(row['sleep']))
        if pd.notna(row.get('pa')):
            point = point.field("pa", str(row['pa']))
        if 'timestamp_raw' in row and pd.notna(row['timestamp_raw']):
            point = point.field("timestamp_raw", float(row['timestamp_raw']))

        point = point.time(row['timestamp'])

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        success_count += 1
        logging.info(f"[{i}] Written point for userID={row['ID']} @ {row['timestamp']}")
    except Exception as e:
        error_count += 1
        logging.error(f"[{i}] Failed to write point for userID={row['ID']}: {e}")

client.close()

print(f"✅ Done. Successfully wrote {success_count} rows. Failed: {error_count}")
logging.info(f"✅ Total written: {success_count} | Failed: {error_count}")

