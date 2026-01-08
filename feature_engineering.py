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

# =====================================================
# PATH CONFIG
# =====================================================

FHIR_DIR = Path(r"D:\capstone project\synthea_fhir")
OUTPUT_FILE = Path(r"D:\capstone project\processed\patients_final.json")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# =====================================================
# DOMAIN CONFIG
# =====================================================

LAB_BOUNDS = {
    "Hemoglobin A1c": (4.0, 12.0),
    "Creatinine": (0.5, 6.0),
    "Total Cholesterol": (100, 350),
    "Systolic Blood Pressure": (90, 200)
}

SEVERITY_MAP = {
    "mild": 0,
    "moderate": 1,
    "severe": 2
}

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

# =====================================================
# HELPERS
# =====================================================

def min_max(value, lo, hi):
    if value is None:
        return 0
    if hi == lo:
        return 0.0
    return round((value - lo) / (hi - lo), 4)

def calculate_age(birth_date):
    if not birth_date:
        return 0
    birth = datetime.strptime(birth_date, "%Y-%m-%d").date()
    today = date.today()
    return today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))

# =====================================================
# FEATURE ENGINEERING
# =====================================================

def build_features(bundle):
    missing_value_count = 0          # nulls + missing observations
    replaced_with_zero_count = 0     # only null -> 0
    patient_has_missing = False

    patient = parse_patient(bundle)
    if not patient:
        return None, 0, 0, False

    patient_numeric_id = patient["id"].split("-")[-1]

    conditions = parse_conditions(bundle)
    encounters = parse_encounters(bundle)
    observations = parse_observations(bundle)

    # =================================================
    # HANDLE MISSING OBSERVATIONS
    # =================================================

    if len(observations) == 0:
        # No labs recorded at all
        missing_value_count += len(LAB_BOUNDS)
        patient_has_missing = True

    # Replace null observation values with 0
    for obs in observations:
        if obs.get("value") is None:
            missing_value_count += 1
            replaced_with_zero_count += 1
            obs["value"] = 0
            patient_has_missing = True

    # =================================================
    # BASIC FEATURES
    # =================================================

    age = calculate_age(patient.get("birthDate"))
    visit_count = len(encounters)
    lab_count = len(observations)

    chronic_flag = int(any(c["name"] in CHRONIC_CONDITIONS for c in conditions))
    active_status = patient.get("active")

    max_severity = max(
        (SEVERITY_MAP.get(c["severity"].lower()) for c in conditions if c.get("severity")),
        default=0
    )

    # =================================================
    # LAB AGGREGATION
    # =================================================

    lab_values = {}
    for obs in observations:
        lab_values.setdefault(obs["lab"], []).append(obs["value"])

    normalized_labs = {}
    for lab, values in lab_values.items():
        if lab in LAB_BOUNDS:
            non_zero = [v for v in values if v > 0]
            avg = sum(non_zero) / len(non_zero) if non_zero else 0
            lo, hi = LAB_BOUNDS[lab]
            normalized_labs[lab] = min_max(avg, lo, hi)

    # =================================================
    # OUTPUT RECORD
    # =================================================

    record = {
        "patient_id": patient_numeric_id,
        "ml_features": {
            "age_norm": min_max(age, 18, 90),
            "visit_count_norm": min_max(visit_count, 1, 15),
            "lab_count_norm": min_max(lab_count, 0, 20),
            "lab_values_norm": normalized_labs,
            "chronic_flag": chronic_flag,
            "max_severity": max_severity
        },
        "narrative_fields": {
            "birthDate": patient.get("birthDate"),
            "active_status": active_status,
            "conditions": conditions,
            "encounters": encounters,
            "lab_observations": observations
        }
    }

    return record, missing_value_count, replaced_with_zero_count, patient_has_missing

# =====================================================
# MAIN
# =====================================================

def main():
    records = []

    total_missing_values = 0
    total_replaced_with_zero = 0
    patients_with_missing = 0

    files = list(FHIR_DIR.glob("patient_*.json"))
    print(f"Found {len(files)} patient files in {FHIR_DIR}")

    for idx, file in enumerate(files, start=1):
        try:
            with open(file, "r", encoding="utf-8", errors="ignore") as f:
                bundle = json.load(f)

            record, missing, replaced, has_missing = build_features(bundle)

            if record:
                records.append(record)
                total_missing_values += missing
                total_replaced_with_zero += replaced
                if has_missing:
                    patients_with_missing += 1

        except Exception as e:
            print(f"[SKIPPED] {file.name} → {type(e).__name__}: {e}")

        if idx % 1000 == 0:
            print(f"Processed {idx}/{len(files)} files")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    # =================================================
    # FINAL REPORT
    # =================================================

    print("\n Feature engineering completed successfully")
    print(f"Patients retained: {len(records)}")
    print(f" Total missing values detected: {total_missing_values}")
    print(f" Total missing values replaced with 0: {total_replaced_with_zero}")
    print(f" Patients with at least one missing value: {patients_with_missing}")
    print(f"Output saved to: {OUTPUT_FILE}")

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    main()
