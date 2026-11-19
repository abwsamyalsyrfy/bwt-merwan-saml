import os
import json
import math
import collections
import firebase_admin
from firebase_admin import firestore

# --- Configuration ---
PROJECT_ID = "bd-saml-marwan-2026"
DATA_PATH = os.path.join(os.path.dirname(__file__), "bot_data")
INDEX_FILE = "index.json"
CONFIG_COLLECTION = "library_config"
CONTENT_COLLECTION = "library_series_content"

# Firestore's max document size is 1 MiB (1,048,576 bytes).
# We'll use a safer, slightly smaller limit to account for overhead.
MAX_DOC_SIZE = 1000000

def get_doc_size(data):
    """Estimates the size of the document by encoding it to a UTF-8 string."""
    return len(json.dumps(data, ensure_ascii=False).encode('utf-8'))

def upload_library_to_firestore():
    """Reads local JSON data and uploads it to Firestore, sharding large series."""
    print(f"Starting data upload for project: {PROJECT_ID}")
    try:
        firebase_admin.initialize_app(options={"projectId": PROJECT_ID})
        db = firestore.client()
        print("Successfully connected to Firestore.")

        index_path = os.path.join(DATA_PATH, INDEX_FILE)
        print(f"Reading index file from: {index_path}")
        # Use OrderedDict to preserve the sequence of series keys from index.json
        with open(index_path, 'r', encoding='utf-8') as f:
            index_data = json.load(f, object_pairs_hook=collections.OrderedDict)

        if not index_data:
            raise ValueError("Index data is empty. Aborting.")

        # --- Process and Upload Each Series ---
        content_batch = db.batch()

        # Iterate through index keys and assign an explicit sort_order based on file position
        for idx, (series_id, series_info) in enumerate(index_data.items()):
            series_info['sort_order'] = idx
            series_file = os.path.join(DATA_PATH, f"{series_id}.json")
            print(f"Processing series '{series_id}' (order: {idx}) from '{series_file}'...")

            if not os.path.exists(series_file):
                print(f"Warning: Data file not found. Skipping.")
                series_info['sharded'] = False # Mark as not sharded
                continue

            with open(series_file, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            
            # The JSON loading naturally preserves all fields including multimedia IDs
            document_data = {"messages": messages}
            doc_size = get_doc_size(document_data)

            if doc_size <= MAX_DOC_SIZE:
                # --- Standard (Small) Series --- 
                print(f"  -> Size ({doc_size} bytes) is within limits. Uploading as a single document.")
                series_info['sharded'] = False
                doc_ref = db.collection(CONTENT_COLLECTION).document(series_id)
                content_batch.set(doc_ref, document_data)
            else:
                # --- Sharding for Large Series --- 
                print(f"  -> Size ({doc_size} bytes) exceeds limit. Sharding required.")
                
                # Estimate chunks needed, assuming roughly even message sizes
                num_chunks = math.ceil(doc_size / MAX_DOC_SIZE)
                chunk_size = math.ceil(len(messages) / num_chunks)
                print(f"  -> Splitting into {num_chunks} parts of ~{chunk_size} messages each.")
                
                series_info['sharded'] = True
                series_info['parts'] = num_chunks

                for i in range(num_chunks):
                    part_id = f"{series_id}_part_{i}"
                    start_index = i * chunk_size
                    end_index = start_index + chunk_size
                    chunk_messages = messages[start_index:end_index]
                    
                    part_data = {"messages": chunk_messages}
                    part_doc_ref = db.collection(CONTENT_COLLECTION).document(part_id)
                    content_batch.set(part_doc_ref, part_data)
                    print(f"    - Staged part {i} ({len(chunk_messages)} messages) for upload to '{part_id}'.")

        # --- Commit all content changes (standard and sharded) ---
        print("\nCommitting content batch to Firestore...")
        content_batch.commit()
        print("Content batch successfully committed.")

        # --- Upload the final, modified index ---
        print("Uploading final index with sharding metadata and sort order...")
        index_ref = db.collection(CONFIG_COLLECTION).document("index")
        # Convert OrderedDict back to standard dict for Firestore upload while having injected sort_order
        index_ref.set(dict(index_data))
        print("Index successfully uploaded.")

        print("\n--- Upload Complete! ---")
        print("Firestore database is now populated with potentially sharded data.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    upload_library_to_firestore()