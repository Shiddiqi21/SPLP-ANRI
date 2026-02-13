
import sys
import os
sys.path.append(os.getcwd())

try:
    print("Attempting to import app.api.stats_routes...")
    from app.api import stats_routes
    print("SUCCESS: Import successful.")
except Exception as e:
    print(f"ERROR: Import failed: {e}")
except SyntaxError as e:
    print(f"ERROR: Syntax Error: {e}")
