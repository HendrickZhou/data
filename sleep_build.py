import os
import pandas as pd
import logging
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# -----------------------
# Configs
# -----------------------
CSV_FILE = "./raw/morining_ema_day_aggre_sleepqual.csv"
MEASUREMENT = "sleep_quality"
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
df = pd.read_csv(CSV_FILE)

# Filter for specific user_id
df = df[df['id'] != USER_ID_TO_TEST]
df.reset_index(drop=True, inplace=True)

df['start_date'] = pd.to_datetime(df['start_date'], format="%m/%d/%Y")
df['start_date'] = df['start_date'] + pd.Timedelta(hours=7)
df['start_date'] = df['start_date'].dt.tz_localize('UTC')  # Ensure proper timezone

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
        point = (
        Point(MEASUREMENT)
        .tag("userID", int(row['id']))
        .time(row['start_date'], WritePrecision.NS)
        )

        if pd.notna(row.get('sleep_quality')):
            point = point.field("sq", float(row['sleep_quality']))

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        success_count += 1
        logging.info(f"[{i}] Written point for userID={row['id']}")
    except Exception as e:
        error_count += 1
        logging.error(f"[{i}] Failed to write point for userID={row['id']}: {e}")

client.close()

print(f"✅ Done. Successfully wrote {success_count} rows. Failed: {error_count}")
logging.info(f"✅ Total written: {success_count} | Failed: {error_count}")
