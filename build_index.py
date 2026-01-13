import faiss
import json
import numpy as np
from pymongo import MongoClient
from pathlib import Path

# --- CONFIG ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "synthetic_fhir"
COLLECTION_NAME = "patients"

INDEX_DIR = Path(r"D:\capstone project\faiss_index")
FAISS_INDEX_FILE = INDEX_DIR / "patient.index"
MAPPING_FILE = INDEX_DIR / "index_mapping.json"

def main():
    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME][COLLECTION_NAME]

    print("🔍 Fetching updated embeddings from MongoDB...")
    cursor = collection.find({"embedding": {"$exists": True}}, {"patient_id": 1, "embedding": 1, "vector_id": 1})

    embeddings = []
    index_mapping = {}

    for idx, doc in enumerate(cursor):
        embeddings.append(np.array(doc["embedding"], dtype="float32"))
        index_mapping[str(idx)] = {
            "patient_id": doc["patient_id"],
            "vector_id": doc.get("vector_id")
        }

    if not embeddings:
        print("❌ No embeddings found! Did you run embeddings.py?")
        return

    embeddings = np.vstack(embeddings)
    
    # Use Inner Product for Cosine Similarity (requires normalization)
    faiss.normalize_L2(embeddings)
    index = faiss.IndexFlatIP(embeddings.shape[1])
    index.add(embeddings)

    faiss.write_index(index, str(FAISS_INDEX_FILE))
    with open(MAPPING_FILE, "w") as f:
        json.dump(index_mapping, f, indent=2)

    print(f"✅ FAISS Index built with {index.ntotal} patients.")

if __name__ == "__main__":
    main()