# mongodb/store_patients_mongo.py

import json
from pathlib import Path
from pymongo import MongoClient

# =====================================================
# CONFIG
# =====================================================

INPUT_FILE = Path(r"D:\capstone project\processed\patients_final.json")
MONGO_URI = "mongodb://localhost:27017/"   # Change if needed
DB_NAME = "synthetic_fhir"
COLLECTION_NAME = "patients"

# =====================================================
# CONNECT TO MONGO
# =====================================================

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Optional: Clear existing collection to avoid duplicates
collection.drop()
print(f"Existing collection '{COLLECTION_NAME}' dropped. Starting fresh.")

# =====================================================
# LOAD PATIENT DATA
# =====================================================

if not INPUT_FILE.is_file():
    raise FileNotFoundError(f"{INPUT_FILE} does not exist. Run feature_engineering.py first!")

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    patients = json.load(f)

print(f"Loaded {len(patients)} patients from {INPUT_FILE}")

# =====================================================
# INSERT INTO MONGODB
# =====================================================

for patient in patients:
    # Ensure MongoDB-friendly structure
    collection.insert_one(patient)

print(f" All {len(patients)} patients inserted into MongoDB collection '{COLLECTION_NAME}'")
print(f"DB: '{DB_NAME}', URI: '{MONGO_URI}'")
