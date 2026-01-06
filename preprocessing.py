import json
from pathlib import Path
from datetime import datetime

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
            conditions.append({
                "name": res["code"]["coding"][0]["display"],
                "severity": res.get("severity", {}).get("text"),
                "status": res["clinicalStatus"]["coding"][0]["code"]
            })
    return conditions

def parse_encounters(bundle):
    encounters = {}
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Encounter":
            encounters[res["id"]] = res["period"]["start"]
    return encounters

def parse_observations(bundle):
    observations = []
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Observation":
            observations.append({
                "lab": res["code"]["coding"][0]["display"],
                "value": res.get("valueQuantity", {}).get("value"),
                "date": res.get("effectiveDateTime")
            })
    return observations

# =========================
# FLATTENING LOGIC
# =========================

def flatten_bundle(bundle):
    patient = parse_patient(bundle)
    conditions = parse_conditions(bundle)
    encounters = parse_encounters(bundle)
    observations = parse_observations(bundle)

    if patient is None:
        return None

    flat_record = {
        "patient_id": patient.get("id"),
        "gender": patient.get("gender"),
        "birthDate": patient.get("birthDate"),
        "num_conditions": len(conditions),
        "num_encounters": len(encounters),
        "num_observations": len(observations),
        "conditions": [c["name"] for c in conditions],
        "labs": [o["lab"] for o in observations]
    }

    return flat_record

# =========================
# MAIN (BATCH PROCESSING)
# =========================

def main():
    start_time = datetime.now()
    print(f"Preprocessing started at: {start_time}")
    print(f"Reading FHIR bundles from: {FHIR_DIR}")

    files = sorted(FHIR_DIR.glob("patient_*.json"))
    print(f"Found {len(files)} patient files")

    all_records = []
    skipped_files = 0

    for idx, file in enumerate(files, start=1):
        try:
            with open(file, "r", encoding="utf-8") as f:
                bundle = json.load(f)
                record = flatten_bundle(bundle)
                if record:
                    all_records.append(record)
                else:
                    skipped_files += 1
        except Exception as e:
            skipped_files += 1
            print(f"Skipping {file.name}: {e}")

        # Progress update
        if idx % 500 == 0:
            print(f"Processed {idx}/{len(files)} files...")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\nPreprocessing completed successfully")
    print(f"Finished at: {end_time}")
    print(f"Total runtime: {duration:.2f} seconds")
    print(f"Processed records: {len(all_records)}")
    print(f"Skipped files: {skipped_files}")
    print(f"Output saved to: {OUTPUT_FILE}")

# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    main()
