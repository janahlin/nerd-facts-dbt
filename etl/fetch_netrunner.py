import requests
import pandas as pd

BASE_URL = "https://netrunnerdb.com/api/2.0/public/"
ENDPOINTS = {
    "cards": "cards",
    "cycles": "cycles",
    "packs": "packs",
    "types": "types",
    "factions": "factions"
}

def fetch_data(url):
    """Fetch JSON data from API."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Error fetching {url}: {e}")
        return None

def extract_data():
    """
    Extract all datasets from NetrunnerDB API using predefined endpoints.
    Returns a dictionary of DataFrames.
    """
    extracted_data = {}

    for endpoint, path in ENDPOINTS.items():
        url = BASE_URL + path
        data = fetch_data(url)
        if not data or "data" not in data:
            extracted_data[endpoint] = pd.DataFrame()
            continue

        records = data["data"]
        extracted_data[endpoint] = pd.DataFrame(records)

    return extracted_data  # ✅ Returns a dictionary of DataFrames

if __name__ == "__main__":
    extracted_data = extract_data()
    
    for key, df in extracted_data.items():
        print(f"✅ Extracted {len(df)} records from {key}")
