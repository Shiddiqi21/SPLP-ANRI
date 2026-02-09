"""Test stats API endpoints"""
import requests
import json

base = 'http://127.0.0.1:8000'

print('Testing /api/stats/monthly...')
try:
    r = requests.get(f'{base}/api/stats/monthly?year=2025', timeout=10)
    if r.status_code == 200:
        data = r.json()
        print(f'Success! Table: {data["table_name"]}')
        print(f'  Year: {data["year"]}')
        print(f'  Columns: {data["columns"]}')
        print(f'  Data points: {len(data["data"])} months')
        if data['data']:
            print(f'  Sample: {json.dumps(data["data"][0], indent=2)}')
    else:
        print(f'Error: {r.status_code} - {r.text}')
except Exception as e:
    print(f'Connection error: {e}')
    print('Make sure the server is running!')
