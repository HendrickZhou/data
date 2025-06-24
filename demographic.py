import pandas as pd
from pymongo import MongoClient
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import logging

# --- Config ---
CSV_FILE = "./raw/MI_bot_EMA.csv"
MONGO_URI = "mongodb://root:secret@localhost:27017"
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "mytoken"
INFLUXDB_ORG = "MyOrg"
INFLUXDB_BUCKET = "health_data"

logging.basicConfig(
    filename="demowrite.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.getLogger().addHandler(logging.StreamHandler())

# --- Connect MongoDB ---
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["demographic"]
users_collection = mongo_db["users"]

# --- Connect InfluxDB ---
influx_client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# --- Load CSV ---
df = pd.read_csv(CSV_FILE, sep=",", quotechar='"', encoding='utf-8-sig')

# --- Convert datetime column to proper format ---
df['datetime'] = pd.to_datetime(df['datetime'])

# --- Extract and insert demographics into MongoDB ---
"""
demographic_fields = ['ID', 'dob', 'sex', 'hispanic', 'white', 'married', 'livealone', 'edu', 'prevExperi', 'BMI', 'total_days']

user_docs = {}
for _, row in df.iterrows():
    user_id = str(row['ID'])
    if user_id not in user_docs:
        dob_parse = None
        dob_raw = row['dob']
        if pd.notna(dob_raw):
            dob_parse = datetime.strptime(str(dob_raw), "%m/%d/%y").date().isoformat()
        else:
            dob_parse = None
        user_docs[user_id] = {
            "_id": user_id,
            "dob": dob_parse,
            "sex": row['sex'],
            "ethnicity": {
                "hispanic": row['hispanic'],
                "white": row['white']
            },
            "married": row['married'],
            "livealone": row['livealone'],
            "edu": row['edu'],
            "prevExperi": row['prevExperi'],
            "BMI": row['BMI'],
            "total_days": row['total_days'],
            "age_enrolled": row['age']
        }

logging.info(f"csv file iteration done, ready to write to db")

# Upsert user data into MongoDB
for user_doc in user_docs.values():
    uid = user_doc["_id"]
    logging.info(f"{uid} data point written")
    user_doc = {k: (None if pd.isna(v) else v) for k, v in user_doc.items()}
    users_collection.update_one(
        {"_id": user_doc["_id"]},
        {"$set": user_doc},
        upsert=True
    )

logging.info(f"✅ Inserted {len(user_docs)} unique users into MongoDB")

"""

# --- Write survey responses to InfluxDB ---
survey_fields = [
    "where_now", "whowith_now",
    "mindsharp", "concentrate", "per_cog", "thoughtsemotions", "physical", "present", "mindfulness",
    "feelenergy", "energetic", "joyful", "high_arousal_pos", "calm", "depressed",
    "stressed", "anxious", "negative_affect", "tired", "lonely", "pain", "control", "feel"
]

logging.info(f"ready to write to influxdb")

success_count = 0
error_count = 0

for i, row in df.iterrows():
    try:
        user_id = int(row["ID"])
        timestamp = pd.to_datetime(row["datetime"])

        point = Point("ema_data").tag("userID", user_id)

        if pd.notna(row.get('Day')):
            point = point.field('day', row['Day'])
        if pd.notna(row.get('day_in_study')):
            point = point.field('day_in_study', int(row['day_in_study']))

        skipped_fields = []
        for field in survey_fields:
            value = row.get(field)
            if pd.notna(value):
                point = point.field(field, int(value))
            else:
                skipped_fields.append(field)
        if skipped_fields:
            logging.info(f"Skipped {len(skipped_fields)} missing fields for user {user_id} at {row['datetime']}")

        # Combine StartDate and StartTime
        start_str = f"{row['StartDate']} {row['StartTime']}"
        starttime = pd.to_datetime(start_str, errors='coerce')
        starttime_iso = starttime.isoformat() if pd.notna(starttime) else None
        # Add extra time field as string
        if starttime_iso:
            point = point.field("starttime_str", starttime_iso)

        point = point.time(timestamp, WritePrecision.S)

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        success_count += 1
        logging.info(f"[{i}] Written point for userID={row['ID']} @ {timestamp}")
    except Exception as e:
        error_count += 1
        logging.error(f"[{i}] Failed to write point for userID={row['ID']}: {e}")

print(f"✅ Done. Successfully wrote {success_count} rows. Failed: {error_count}")
logging.info(f"✅ Total written: {success_count} | Failed: {error_count}")
