import json
import random
import uuid
import argparse
from datetime import date, timedelta
from pathlib import Path
import numpy as np

# CONFIG

OUTPUT_DIR = Path(r"D:\capstone project\synthea_fhir")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MIN_VISITS = 3
MAX_VISITS = 8
MISSING_DATA_PROB = 0.15
NOISE_STD = 0.05

SEVERITIES = ["mild", "moderate", "severe"]

# MEDICAL KNOWLEDGE

CONDITIONS = {
    "Type 2 Diabetes Mellitus": "E11.9",
    "Hypertension": "I10",
    "Coronary Artery Disease": "I25.10",
    "Chronic Kidney Disease": "N18.9",
    "Asthma": "J45.909",
    "COPD": "J44.9",
    "Hypothyroidism": "E03.9",
    "Hyperlipidemia": "E78.5",
    "Anemia": "D64.9",
    "Depression": "F32.9",
    "Obesity": "E66.9",
    "Osteoarthritis": "M19.90"
}

LABS = {
    "Hemoglobin A1c": ("4548-4", "%", (6.0, 11.0)),
    "Systolic Blood Pressure": ("8480-6", "mmHg", (120, 190)),
    "Creatinine": ("2160-0", "mg/dL", (0.8, 5.0)),
    "Total Cholesterol": ("2093-3", "mg/dL", (150, 320))
}

CONDITION_LABS = {
    "Type 2 Diabetes Mellitus": ["Hemoglobin A1c"],
    "Hypertension": ["Systolic Blood Pressure"],
    "Chronic Kidney Disease": ["Creatinine"],
    "Hyperlipidemia": ["Total Cholesterol"]
}

# HELPERS

def random_birthdate():
    age = random.randint(30, 85)
    return date.today() - timedelta(days=365 * age)

def patient_dates():
    entry = date.today() - timedelta(days=random.randint(500, 2500))
    ongoing = random.random() < 0.65
    last_updated = date.today() if ongoing else entry + timedelta(days=random.randint(200, 1200))
    return entry, last_updated, ongoing

def visit_dates(start, n):
    return sorted(start + timedelta(days=random.randint(0, 900)) for _ in range(n))

def noisy(value):
    return round(value * (1 + np.random.normal(0, NOISE_STD)), 2)

# FHIR BUILDERS

def fhir_patient(pid):
    entry, last_updated, ongoing = patient_dates()
    return {
        "resourceType": "Patient",
        "id": pid,
        "gender": random.choice(["male", "female"]),
        "birthDate": random_birthdate().isoformat(),
        "meta": {"lastUpdated": last_updated.isoformat()},
        "extension": [{
            "valueBoolean": ongoing
        }],
        "_recordStart": entry.isoformat()
    }

def fhir_condition(name, icd, severity, ongoing):
    return {
        "resourceType": "Condition",
        "clinicalStatus": {
            "coding": [{
                "code": "active" if ongoing else "resolved"
            }]
        },
        "severity": {"text": severity},
        "code": {
            "coding": [{
                "code": icd,
                "display": name
            }]
        }
    }

def fhir_encounter(eid, visit_date):
    return {
        "resourceType": "Encounter",
        "id": eid,
        "status": "finished",
        "period": {"start": visit_date.isoformat()}
    }

def fhir_observation(lab, visit_date):
    if random.random() < MISSING_DATA_PROB:
        return None

    loinc, unit, (low, high) = LABS[lab]
    return {
        "resourceType": "Observation",
        "effectiveDateTime": visit_date.isoformat(),
        "code": {
            "coding": [{
                "code": loinc,
                "display": lab
            }]
        },
        "valueQuantity": {
            "value": noisy(random.uniform(low, high)),
            "unit": unit
        }
    }

# GENERATOR

def generate_patient_bundle(idx):
    pid = f"patient-{idx:04d}"
    entries = []

    patient = fhir_patient(pid)
    entries.append({"resource": patient})

    selected_conditions = random.sample(
        list(CONDITIONS.items()),
        random.randint(2, 4)
    )

    severities = {c[0]: random.choice(SEVERITIES) for c in selected_conditions}
    ongoing = patient["extension"][0]["valueBoolean"]

    for name, icd in selected_conditions:
        entries.append({
            "resource": fhir_condition(name, icd, severities[name], ongoing)
        })

    start = date.fromisoformat(patient["_recordStart"])
    visits = visit_dates(start, random.randint(MIN_VISITS, MAX_VISITS))

    for i, v in enumerate(visits):
        enc_id = f"enc-{pid}-{i}"
        entries.append({"resource": fhir_encounter(enc_id, v)})

        for condition in severities:
            for lab in CONDITION_LABS.get(condition, []):
                obs = fhir_observation(lab, v)
                if obs:
                    obs["encounter"] = {"reference": f"Encounter/{enc_id}"}
                    entries.append({"resource": obs})

    return {
        "resourceType": "Bundle",
        "id": str(uuid.uuid4()),
        "type": "collection",
        "entry": entries
    }

# CLI ARGUMENTS

def parse_args():
    parser = argparse.ArgumentParser(description="Synthetic FHIR Patient Generator")
    parser.add_argument(
        "--num_patients",
        type=int,
        default=100,
        help="Number of patients to generate"
    )
    return parser.parse_args()

# RUN

if __name__ == "__main__":
    args = parse_args()

    for i in range(1, args.num_patients + 1):
        bundle = generate_patient_bundle(i)
        with open(OUTPUT_DIR / f"patient_{i:04d}.json", "w") as f:
            json.dump(bundle, f, indent=2)

    print(f"Generated {args.num_patients} synthetic FHIR patient bundles at {OUTPUT_DIR}")
