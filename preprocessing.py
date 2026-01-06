import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# =========================
# CONFIG
# =========================

FHIR_DIR = Path(r"D:\capstone project\synthea_fhir")
OUTPUT_FILE = Path(r"D:\capstone project\processed\patients_flat.json")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# =========================
# FHIR PARSING FUNCTIONS
# =========================

def parse_patient(bundle):
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Patient":
            return res
    return None

def parse_conditions(bundle):
    conditions = []
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Condition":
            conditions.append(res["code"]["coding"][0]["display"])
    return conditions

def parse_observations(bundle):
    """
    Collect numeric lab values
    """
    labs = []
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Observation":
            if "valueQuantity" in res:
                labs.append({
                    "lab": res["code"]["coding"][0]["display"],
                    "value": res["valueQuantity"]["value"],
                    "date": res.get("effectiveDateTime")
                })
    return labs

# =========================
# FLATTENING LOGIC
# =========================

def flatten_bundle(bundle):
    patient = parse_patient(bundle)
    if patient is None:
        return None

    conditions = parse_conditions(bundle)
    observations = parse_observations(bundle)

    # Aggregate lab values per patient (mean)
    lab_values = defaultdict(list)
    for obs in observations:
        lab_values[obs["lab"]].append(obs["value"])

    lab_means = {
        lab: round(sum(values) / len(values), 3)
        for lab, values in lab_values.items()
    }

    return {
        "patient_id": patient["id"],
        "gender": patient.get("gender"),
        "birthDate": patient.get("birthDate"),
        "conditions": conditions,
        "num_conditions": len(conditions),
        "lab_values": lab_means
    }

# =========================
# NORMALIZATION
# =========================

def normalize_lab_values(records):
    """
    Min–Max normalization across patients
    """
    lab_min_max = {}

    # Collect min/max
    for record in records:
        for lab, value in record["lab_values"].items():
            if lab not in lab_min_max:
                lab_min_max[lab] = {"min": value, "max": value}
            else:
                lab_min_max[lab]["min"] = min(lab_min_max[lab]["min"], value)
                lab_min_max[lab]["max"] = max(lab_min_max[lab]["max"], value)

    # Apply normalization
    for record in records:
        normalized = {}
        for lab, value in record["lab_values"].items():
            min_v = lab_min_max[lab]["min"]
            max_v = lab_min_max[lab]["max"]
            normalized[lab] = round(
                (value - min_v) / (max_v - min_v), 4
            ) if max_v > min_v else 0.0

        record["lab_values"] = normalized

    return records

# =========================
# MAIN
# =========================

def main():
    start_time = datetime.now()
    print(f"Started at: {start_time}")

    records = []
    files = sorted(FHIR_DIR.glob("patient_*.json"))

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            bundle = json.load(f)
            record = flatten_bundle(bundle)
            if record:
                records.append(record)

    records = normalize_lab_values(records)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


    end_time = datetime.now()
    print(f"Finished at: {end_time}")
    print(f"Total patients processed: {len(records)}")
    print(f"Output saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
