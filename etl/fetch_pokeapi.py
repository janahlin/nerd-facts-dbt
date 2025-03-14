import requests
import pandas as pd
import json

BASE_URL = "https://pokeapi.co/api/v2/"
ENDPOINTS = {
    "pokemon": "pokemon",
    "types": "type",
    "abilities": "ability",
    "moves": "move",
    "items": "item"
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

def fetch_all_pages(endpoint):
    """Fetch all pages of data from a paginated PokeAPI endpoint."""
    records = []
    url = f"{BASE_URL}{endpoint}?limit=100"  # Start with the first page

    while url:
        data = fetch_data(url)
        if not data or "results" not in data:
            break  # Stop if no valid data

        records.extend(data["results"])  # Collect all records
        url = data.get("next")  # Update URL for next page

    return records

def extract_uid(value):
    """
    Extracts IDs from PokeAPI URLs.
    - If `value` is a single URL, return the ID.
    - If `value` is a list of URLs, return a JSON array of IDs.
    """
    if isinstance(value, str) and value.startswith(BASE_URL):
        return value.rstrip("/").rsplit("/", 1)[-1]  # Extract ID from URL
    
    elif isinstance(value, list):
        uids = [extract_uid(v) for v in value if isinstance(v, str)]
        return json.dumps(uids)  # Store as JSONB
    
    return value

def clean_links(record):
    """Cleans record fields by replacing URL references with IDs."""
    return {key: extract_uid(value) for key, value in record.items()}

def extract_data():
    """
    Extract all datasets from PokeAPI using predefined endpoints.
    Returns a dictionary of DataFrames.
    """
    extracted_data = {}

    for endpoint, path in ENDPOINTS.items():
        print(f"🔄 Fetching data from {endpoint}...")
        all_entries = fetch_all_pages(path)

        records = []
        for entry in all_entries:
            detail_data = fetch_data(entry["url"]) if "url" in entry else None
            if detail_data:
                properties = clean_links(detail_data)  # Convert links to IDs
                properties["id"] = detail_data["id"]
                records.append(properties)

        df = pd.DataFrame(records)
        print(f"✅ Extracted {len(df)} records from {endpoint}")

        # Drop rows where `id` is missing
        if not df.empty:
            df = df.dropna(subset=["id"]).astype({"id": "int64"})        

        extracted_data[endpoint] = df

    return extracted_data  # ✅ Returns a dictionary of DataFrames

if __name__ == "__main__":
    extracted_data = extract_data()
    
    for key, df in extracted_data.items():
        print(f"✅ Extracted {len(df)} records from {key}")
