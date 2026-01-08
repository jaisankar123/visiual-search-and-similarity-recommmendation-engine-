# sentence_builder.py

import json
from pymongo import MongoClient
from datetime import datetime
from pathlib import Path

# =====================================================
# CONFIG
# =====================================================

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "synthetic_fhir"
COLLECTION_NAME = "patients"

OUTPUT_FILE = Path(r"D:\capstone project\patient_sentences.json")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# =====================================================
# CONNECT TO MONGO
# =====================================================

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

print("Connected to MongoDB")

# =====================================================
# HELPERS
# =====================================================

def is_valid(value):
    """True if value is meaningful (not None, 0, empty)."""
    return value not in (None, 0, "", [], {})


def calculate_age(birth_date):
    if not is_valid(birth_date):
        return None
    birth = datetime.strptime(birth_date, "%Y-%m-%d").date()
    today = datetime.today().date()
    return today.year - birth.year - (
        (today.month, today.day) < (birth.month, birth.day)
    )

# =====================================================
# SENTENCE BUILDER
# =====================================================

def build_patient_sentence(patient):
    narrative = patient.get("narrative_fields", {})
    if not is_valid(narrative):
        return None

    labs = narrative.get("lab_observations", [])
    conditions = narrative.get("conditions", [])
    encounters = narrative.get("encounters", [])

    sentence_parts = []

    # -------- BASIC INFO --------
    birth_date = narrative.get("birthDate")
    age = calculate_age(birth_date)
    status = narrative.get("active_status")

    if is_valid(age) and is_valid(birth_date) and is_valid(status):
        status_str = "active" if status else "inactive"
        sentence_parts.append(
            f"Jane Smith is a {age}-year-old patient (born on {birth_date}) "
            f"whose current status is {status_str}."
        )

    # -------- VISITS --------
    if is_valid(encounters):
        sentence_parts.append(
            f"The patient has had {len(encounters)} clinical visits."
        )

    # -------- CONDITIONS --------
    valid_conditions = [
        c["name"] for c in conditions if is_valid(c.get("name"))
    ]

    if valid_conditions:
        sentence_parts.append(
            f"Medical history includes {', '.join(valid_conditions)}."
        )

    # -------- LABS --------
    valid_labs = [
        f"{lab.get('lab')}: {lab.get('value')}"
        for lab in labs
        if is_valid(lab.get("lab")) and is_valid(lab.get("value"))
    ]

    if valid_labs:
        sentence_parts.append(
            "Lab results from the last visit: " + ", ".join(valid_labs) + "."
        )

    if not sentence_parts:
        return None

    return " ".join(sentence_parts)

# =====================================================
# MAIN
# =====================================================

def main(limit=None):
    cursor = collection.find()
    if limit:
        cursor = cursor.limit(limit)

    sentences_output = []

    total = 0
    generated = 0
    skipped = 0

    for patient in cursor:
        total += 1
        sentence = build_patient_sentence(patient)

        if sentence:
            generated += 1
            sentences_output.append({
                "patient_id": patient.get("patient_id"),
                "sentence": sentence
            })
        else:
            skipped += 1

    # -------- SAVE FILE --------
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(sentences_output, f, indent=2)

    # -------- SUMMARY --------
    print("\nSUMMARY")
    print("=" * 80)
    print(f"Total patients read       : {total}")
    print(f"Sentences generated       : {generated}")
    print(f"Patients skipped          : {skipped}")
    print(f"Sentence file saved to    : {OUTPUT_FILE}")

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    main()
