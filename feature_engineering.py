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
        return None
    if hi == lo:
        return 0.0
    return round((value - lo) / (hi - lo), 4)

def calculate_age(birth_date):
    if not birth_date:
        return None
    birth = datetime.strptime(birth_date, "%Y-%m-%d").date()
    today = date.today()
    return today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))

def compute_lab_trends(observations):
    history = {}
    for obs in observations:
        if obs.get("value") is not None:
            history.setdefault(obs["lab"], []).append((obs["date"], obs["value"]))

    trends = {}
    for lab, values in history.items():
        if len(values) < 2:
            continue
        values.sort()
        delta = values[-1][1] - values[0][1]
        trends[lab] = "increasing" if delta > 0 else "decreasing" if delta < 0 else "stable"
    return trends

# =====================================================
# FEATURE ENGINEERING
# =====================================================

def build_features(bundle):
    patient = parse_patient(bundle)
    if not patient:
        return None

    # --- keep only numeric part of patient_id
    patient_numeric_id = patient["id"].split("-")[-1]

    conditions = parse_conditions(bundle)
    encounters = parse_encounters(bundle)
    observations = parse_observations(bundle)

    # ---- basic numeric features
    age = calculate_age(patient.get("birthDate"))
    visit_count = len(encounters)
    lab_count = len(observations)

    # ---- binary features
    chronic_flag = int(any(c["name"] in CHRONIC_CONDITIONS for c in conditions))
    active_status = patient.get("active")

    # ---- ordinal severity
    max_severity = max(
        (SEVERITY_MAP.get(c["severity"].lower()) for c in conditions if c.get("severity")),
        default=None
    )

    # ---- aggregate lab values
    lab_values = {}
    missing_lab_count = 0
    for obs in observations:
        if obs.get("value") is not None:
            lab_values.setdefault(obs["lab"], []).append(obs["value"])
        else:
            missing_lab_count += 1

    # ---- normalize labs
    normalized_labs = {}
    for lab, values in lab_values.items():
        if lab in LAB_BOUNDS and values:
            avg = sum(values) / len(values)
            lo, hi = LAB_BOUNDS[lab]
            normalized_labs[lab] = min_max(avg, lo, hi)

    # ---- narrative_fields (keep all raw info)
    narrative_fields = {
        "birthDate": patient.get("birthDate"),
        "active_status": active_status,
        "conditions": [
            {"name": c["name"], "severity": c.get("severity"), "status": c.get("status"), "icd": c.get("icd","")}
            for c in conditions
        ],
        "encounters": [
            {"id": idx+1, "status": "finished", "date": None}  # Can be filled if encounter dates are available in parser
            for idx in range(visit_count)
        ],
        "lab_observations": observations
    }

    return {
        "patient_id": patient_numeric_id,  # numeric only
        "ml_features": {
            "age_norm": min_max(age, 18, 90),
            "visit_count_norm": min_max(visit_count, 1, 15),
            "lab_count_norm": min_max(lab_count, 0, 20),
            "lab_values_norm": normalized_labs,
            "chronic_flag": chronic_flag,
            "max_severity": max_severity,
            "missing_lab_count": missing_lab_count
        },
        "narrative_fields": narrative_fields,
        "lab_trends": compute_lab_trends(observations)
    }

# =====================================================
# MAIN
# =====================================================

def main():
    records = []

    files = list(FHIR_DIR.glob("patient_*.json"))
    print(f"Found {len(files)} patient files in {FHIR_DIR}")

    for idx, file in enumerate(files, start=1):
        if not file.is_file():
            continue
        try:
            with open(file, "r", encoding="utf-8", errors="ignore") as f:
                bundle = json.load(f)

            record = build_features(bundle)
            if record:
                records.append(record)

        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[SKIPPED] {file.name} → {type(e).__name__}: {e}")
            continue

        if idx % 1000 == 0:
            print(f"Processed {idx}/{len(files)} files")

    total_missing = sum(r["ml_features"].get("missing_lab_count",0) for r in records)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    print("\n✅ Feature engineering completed successfully")
    print(f"Patients retained: {len(records)}")
    
    print(f"Output saved to: {OUTPUT_FILE}")

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    main()
