# preprocessing/feature_engineering.py

import json
from pathlib import Path
from datetime import datetime, date
from fhir_parser import (
    parse_patient,
    parse_conditions,
    parse_encounters,
    parse_observations
)

# =========================
# PATH CONFIG
# =========================
FHIR_DIR = Path(r"D:\capstone project\synthea_fhir")
OUTPUT_FILE = Path(r"D:\capstone project\processed\patients_final.json")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# =========================
# NORMALIZATION RANGES
# (Used for Min–Max Scaling)
# =========================
LAB_BOUNDS = {
    "Hemoglobin A1c": (4.0, 12.0),
    "Creatinine": (0.5, 6.0),
    "Total Cholesterol": (100, 350),
    "Systolic Blood Pressure": (90, 200)
}

# =========================
# CONDITION SEVERITY ENCODING
# mild → 0, moderate → 1, severe → 2
# =========================
SEVERITY_MAP = {"mild": 0, "moderate": 1, "severe": 2}

# =========================
# CHRONIC CONDITION LIST
# Used for chronic_flag feature
# =========================
CHRONIC_CONDITIONS = {
    "Type 2 Diabetes Mellitus",
    "Hypertension",
    "Chronic Kidney Disease",
    "COPD",
    "Asthma",
    "Hypothyroidism",
    "Hyperlipidemia",
    "Depression",
    "Obesity",
    "Osteoarthritis",
    "Coronary Artery Disease",
    "Anemia"
}

# =========================================================
# NORMALIZATION FUNCTION (MIN–MAX SCALING)
# Handles missing values by returning None
# =========================================================
def min_max(value, lo, hi):
    if value is None:
        return None          # ⬅ Missing value handling
    return round((value - lo) / (hi - lo), 4)  # ⬅ Normalization


# =========================================================
# AGE CALCULATION (RAW FEATURE)
# =========================================================
def calculate_age(birth_date):
    if not birth_date:
        return None          # ⬅ Missing DOB handling
    birth = datetime.strptime(birth_date, "%Y-%m-%d").date()
    today = date.today()
    return today.year - birth.year - (
        (today.month, today.day) < (birth.month, birth.day)
    )


# =========================================================
# LAB TREND EXTRACTION
# increasing / decreasing / stable
# =========================================================
def compute_lab_trends(observations):
    history = {}

    # Group lab values by lab name
    for obs in observations:
        if obs["value"] is not None:  # ⬅ Missing lab value handling
            history.setdefault(obs["lab"], []).append(
                (obs["date"], obs["value"])
            )

    trends = {}
    for lab, values in history.items():
        if len(values) < 2:
            continue  # Not enough data to compute trend

        values.sort()
        delta = values[-1][1] - values[0][1]

        trends[lab] = (
            "increasing" if delta > 0 else
            "decreasing" if delta < 0 else
            "stable"
        )

    return trends


# =========================================================
# MAIN FEATURE ENGINEERING PIPELINE
# =========================================================
def build_features(bundle):

    # ---------- Parsing ----------
    patient = parse_patient(bundle)
    if not patient:
        return None

    conditions = parse_conditions(bundle)
    encounters = parse_encounters(bundle)
    observations = parse_observations(bundle)

    # ---------- Raw feature extraction ----------
    age = calculate_age(patient.get("birthDate"))
    visit_count = len(encounters)
    lab_count = len(observations)

    # ---------- Binary chronic flag ----------
    chronic_flag = int(any(
        c["name"] in CHRONIC_CONDITIONS for c in conditions
    ))  # ⬅ 1 = chronic, 0 = non-chronic

    # ---------- Aggregate lab values ----------
    lab_values = {}
    for obs in observations:
        lab_values.setdefault(obs["lab"], []).append(obs["value"])

    # ---------- Normalize lab values ----------
    normalized_labs = {}
    for lab, values in lab_values.items():
        if lab in LAB_BOUNDS:
            valid_values = [v for v in values if v is not None]

            if not valid_values:
                continue  # ⬅ Missing lab handling

            avg = sum(valid_values) / len(valid_values)
            lo, hi = LAB_BOUNDS[lab]

            normalized_labs[lab] = min_max(avg, lo, hi)
            # ⬆ Normalized lab value

    # ---------- Max severity encoding ----------
    max_severity = max(
        (SEVERITY_MAP.get(c["severity"].lower())
         for c in conditions if c["severity"]),
        default=None            # ⬅ Missing severity handling
    )

    # ---------- Final ML-ready patient record ----------
    return {
        "patient_id": patient["id"],
        "gender": patient.get("gender"),
        "birthdate":patient.get("birthDate")

        # ⬇ Normalized demographic & utilization features
        "age": min_max(age, 18, 90),
        "visit_count": min_max(visit_count, 1, 15),
        "lab_count": min_max(lab_count, 0, 20),

        # ⬇ Engineered clinical features
        "chronic_flag": chronic_flag,
        "max_severity": max_severity,

        # ⬇ Categorical info (kept non-normalized)
        "conditions": [c["name"] for c in conditions],

        # ⬇ Normalized labs + trends
        "lab_values": normalized_labs,
        "lab_trends": compute_lab_trends(observations)
    }


# =========================================================
# BATCH PROCESSING
# =========================================================
def main():
    records = []

    for file in FHIR_DIR.glob("patient_*.json"):
        with open(file, "r", encoding="utf-8") as f:
            bundle = json.load(f)
            record = build_features(bundle)
            if record:
                records.append(record)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    print(f"Saved {len(records)} patients → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
