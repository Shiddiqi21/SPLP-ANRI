import sys
import os
import time
sys.path.append(os.getcwd())

from app.services.data_service import data_service

def benchmark():
    print("Benchmarking get_statistics...")
    start = time.time()
    stats = data_service.get_statistics()
    end = time.time()
    print(f"Time taken: {end - start:.4f} seconds")
    print(f"Stats: {stats}")

if __name__ == "__main__":
    benchmark()
