import faiss
import json
import numpy as np
from pymongo import MongoClient
from pathlib import Path
from tabulate import tabulate

# --- CONFIG ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "synthetic_fhir"
COLLECTION_NAME = "patients"

INDEX_DIR = Path(r"D:\capstone project\faiss_index")
FAISS_INDEX_FILE = INDEX_DIR / "patient.index"
MAPPING_FILE = INDEX_DIR / "index_mapping.json"



def main():
    raw_id = input("Enter patient_id (e.g., 0001): ").strip()
    patient_id = raw_id.zfill(4) 

    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME][COLLECTION_NAME]

    # Find the target patient
    patient = collection.find_one({"patient_id": patient_id})
    if not patient or "embedding" not in patient:
        print(f"❌ Patient {patient_id} not found or missing embedding.")
        return

    # Load FAISS and Mapping
    index = faiss.read_index(str(FAISS_INDEX_FILE))
    with open(MAPPING_FILE) as f:
        index_mapping = json.load(f)

    # Search Logic
    search_vector = np.array(patient["embedding"], dtype="float32").reshape(1, -1)
    faiss.normalize_L2(search_vector)
    distances, indices = index.search(search_vector, 6) # Self + top 5

    results = []
    for dist, idx in zip(distances[0], indices[0]):
        map_data = index_mapping.get(str(idx))
        if not map_data or map_data["patient_id"] == patient_id:
            continue
        
        sim_id = map_data["patient_id"]
        sim_doc = collection.find_one({"patient_id": sim_id})
        
        results.append([
            sim_id,
            f"{dist:.4f}",
            sim_doc.get("clinical_sentence", "N/A")[:75] + "..."
        ])

    print(f"\n📌 Patients Similar to {patient_id}:")
    print(tabulate(results, headers=["Patient ID", "Similarity Score", "Clinical Summary"]))

if __name__ == "__main__":
    main()