
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from app.services.schema_inspector import schema_inspector

def list_candidates():
    print("Fetching candidate tables...")
    try:
        tables = schema_inspector.get_candidate_tables()
        print("Candidate Tables found:", tables)
        
        if 'data_arsip' in tables:
            print("SUCCESS: 'data_arsip' is available for registration.")
        else:
            print("WARNING: 'data_arsip' NOT found in candidates. (It might be missing or already registered)")
            
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    list_candidates()
