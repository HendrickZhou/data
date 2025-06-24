import os
import pyreadr
import pandas as pd
import logging
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi
from datetime import timedelta

# -----------------------
# Configs
# -----------------------
RDS_FILE = "./raw/Hang_df.rds"
MEASUREMENT = "health_metrics"
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

df['ID'] = df['ID'].astype(int)

# Clean timestamps
missing_timestamp_count = df['timestamp'].isna().sum()
if missing_timestamp_count > 0:
    logging.warning(f"Dropping {missing_timestamp_count} rows with missing timestamp")
    df = df.dropna(subset=['timestamp'])
df['timestamp'] = pd.to_datetime(df['timestamp'])

# -----------------------
# Influx Client
# -----------------------
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

def fetch_existing_fields(user_id, pa, timestamp):
    # Format start/stop with "Z" for UTC compatibility
    ts_start = timestamp.replace(tzinfo=None).isoformat() + "Z"
    ts_stop = (timestamp + timedelta(seconds=1)).replace(tzinfo=None).isoformat() + "Z"

    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {ts_start}, stop: {ts_stop})
      |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
      |> filter(fn: (r) => r.userID == "{user_id}" and r.pa == "{pa}")
    '''

    tables = query_api.query(flux)
    fields = {}
    for table in tables:
        for record in table.records:
            fields[record.get_field()] = record.get_value()
    return fields

# -----------------------
# Write rows
# -----------------------
print("Ready to write ONLY 'sedtime' and 'standtime' without overwriting other fields. Continue? (y/n)")
if input().lower() != 'y':
    exit()

success_count = 0
error_count = 0

for i, row in df.iterrows():
    try:
        user_id = str(int(row['ID']))
        pa = str(row['pa'])
        ts = row['timestamp']

        # Fetch existing point fields
        existing_fields = fetch_existing_fields(user_id, pa, ts)

        # Add new fields if present
        if pd.notna(row.get('sedtime')):
            existing_fields['sedtime'] = float(row['sedtime'])
        if pd.notna(row.get('standtime')):
            existing_fields['standtime'] = float(row['standtime'])

        # If neither field is available, skip
        if 'sedtime' not in existing_fields and 'standtime' not in existing_fields:
            continue

        point = Point(MEASUREMENT).tag("userID", user_id).tag("pa", pa).time(ts)
        for k, v in existing_fields.items():
            point = point.field(k, v)

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        success_count += 1
        logging.info(f"[{i}] Merged sedtime/standtime for userID={user_id} @ {ts}")
    except Exception as e:
        error_count += 1
        logging.error(f"[{i}] Failed to write merged point for userID={row['ID']}: {e}")

client.close()
print(f"✅ Done. sedtime/standtime added to {success_count} points. Failed: {error_count}")
logging.info(f"✅ sedtime/standtime added: {success_count} | Failed: {error_count}")

