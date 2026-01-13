import os
import json
import time
import datetime
from pathlib import Path
import numpy as np
# =====================================================
# 1. SUPPRESS TENSORFLOW NOISE
# =====================================================
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"

import tensorflow as tf
from pymongo import MongoClient
from transformers import AutoTokenizer, TFAutoModel
from tqdm import tqdm

# =====================================================
# 2. CONFIGURATION
# =====================================================
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "synthetic_fhir"
COLLECTION_NAME = "patients"

SENTENCE_FILE = Path(r"D:\capstone project\nlp\patient_sentences.json")
FAISS_DIR = Path(r"D:\capstone project\faiss_index")
MAPPING_FILE = FAISS_DIR / "index_mapping.json"

MODEL_NAME = "emilyalsentzer/Bio_ClinicalBERT"
EMBEDDING_VERSION = "v1"
BATCH_SIZE = 8  # INCREASED: Process 12 patients per batch
MAX_LENGTH = 512  # Bio_ClinicalBERT limit

# =====================================================
# 3. INITIALIZE MODEL & DATABASE
# =====================================================
print(f"🚀 Loading model: {MODEL_NAME}")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = TFAutoModel.from_pretrained(MODEL_NAME, from_pt=True)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
patients_col = db[COLLECTION_NAME]

# =====================================================
# 4. EMBEDDING FUNCTION (MEAN POOLING)
# =====================================================
def generate_embeddings(texts):
    """
    Converts a batch of clinical summaries into 768-dim embeddings
    using attention-mask-aware mean pooling.
    """
    inputs = tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=MAX_LENGTH,
        return_tensors="tf"
    )

    outputs = model(**inputs)
    token_embeddings = outputs.last_hidden_state

    attention_mask = tf.cast(inputs["attention_mask"], tf.float32)
    mask_expanded = tf.expand_dims(attention_mask, -1)

    summed = tf.reduce_sum(token_embeddings * mask_expanded, axis=1)
    counts = tf.reduce_sum(mask_expanded, axis=1)

    embeddings = summed / tf.maximum(counts, 1e-9)
    return embeddings.numpy()

# =====================================================
# 5. LOAD PATIENT SENTENCES
# =====================================================
if not SENTENCE_FILE.exists():
    raise FileNotFoundError(f"❌ Missing file: {SENTENCE_FILE}")

with open(SENTENCE_FILE, "r", encoding="utf-8") as f:
    patient_data = json.load(f)

print(f"📄 Loaded {len(patient_data)} patient summaries")

# =====================================================
# 6. MAIN PROCESSING LOOP
# =====================================================
index_mapping = {}
start_time = time.time()

# Iterating with the new batch size of 12
for i in tqdm(range(0, len(patient_data), BATCH_SIZE), desc="Embedding patients"):
    batch = patient_data[i : i + BATCH_SIZE]

    batch_texts = [p["sentence"] for p in batch]
    batch_ids = [p["patient_id"] for p in batch]

    vectors = generate_embeddings(batch_texts)

    for j, vector in enumerate(vectors):
        patient_id = batch_ids[j]
        sentence = batch_texts[j]

        vector_id = f"vec_patient-{patient_id}_{EMBEDDING_VERSION}"
        faiss_index = i + j

        # FAISS numeric index → vector_id mapping
        index_mapping[str(faiss_index)] = vector_id

        # MongoDB update (retain all existing fields)
        patients_col.update_one(
            {"patient_id": patient_id},
            {
                "$set": {
                    "vector_id": vector_id,
                    "clinical_sentence": sentence,
                    "embedding": vector.tolist(),
                    "embedding_dim": len(vector),
                    "model_name": MODEL_NAME,
                    "embedding_version": EMBEDDING_VERSION,
                    "last_updated": datetime.datetime.utcnow().isoformat()
                }
            },
            upsert=False
        )

# =====================================================
# 7. SAVE FAISS INDEX MAPPING
# =====================================================
FAISS_DIR.mkdir(parents=True, exist_ok=True)

with open(MAPPING_FILE, "w", encoding="utf-8") as f:
    json.dump(index_mapping, f, indent=2)

# =====================================================
# 8. FINAL STATUS
# =====================================================
elapsed = time.time() - start_time
print("\n✅ Embedding pipeline completed successfully")
print(f"⏱️ Total time: {elapsed:.2f} seconds")
print(f"📂 FAISS mapping saved at: {MAPPING_FILE}")