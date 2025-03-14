import requests
import pandas as pd
import re
import json

BASE_URL = "https://www.swapi.tech/api/"
ENDPOINTS = {
    "people": "people",
    "planets": "planets",
    "starships": "starships",
    "vehicles": "vehicles",
    "species": "species",
    "films": "films"
}

def fetch_data(url):
    """Fetch JSON data from API, handling errors."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Error fetching {url}: {e}")
        return None

def fetch_all_pages(endpoint):
    """Fetch all pages of data from a paginated SWAPI endpoint."""
    records = []
    page = 1  # Start from page 1

    while True:  # Loop until there are no more pages
        url = f"{BASE_URL}{endpoint}?page={page}&limit=10"  # Request the current page        
        data = fetch_data(url)

        if not data or "results" not in data:
            break  # Stop if no valid data

        records.extend(data["results"])  # Collect all records

        if not data.get("next"):                
                break
        else:            
            page += 1

    return records

def extract_uid(value):
    """
    Extracts the UID from a SWAPI URL.
    - If `value` is a single URL, return the UID.
    - If `value` is a list of URLs, return a JSONB-compatible list of UIDs.
    - If the property is named 'url', return it as is.
    """
    if isinstance(value, str):
        if value.startswith("https://www.swapi.tech/api/") and not value.endswith("/"):
            return value.rsplit("/", 1)[-1]  # Extract UID
        return value  # Keep other strings untouched

    elif isinstance(value, list):
        uids = [extract_uid(v) for v in value if isinstance(v, str)]
        return json.dumps(uids)  # Convert list to JSON string for JSONB storage

    return value  # Return unchanged if not a string or list

def clean_links(record):
    """
    Cleans SWAPI record fields by replacing URL references with UIDs,
    except for the 'url' property, which is kept as is.
    """
    return {key: extract_uid(value) if key != "url" else value for key, value in record.items()}



def extract_data():
    """
    Extract all datasets from SWAPI using predefined endpoints.
    Returns a dictionary of DataFrames.
    """
    extracted_data = {}

    for endpoint, path in ENDPOINTS.items():
        
        records = []        
        
        if endpoint == "films":
            # ✅ Films data is structured differently, use single fetch
            url = BASE_URL + path
            data = fetch_data(url)

            if data and "result" in data:
                for entry in data["result"]:
                    properties = entry["properties"]
                    records.append({
                        "id": int(entry["uid"]),  # Use UID as ID
                        "title": properties.get("title"),
                        "episode_id": properties.get("episode_id"),
                        "director": properties.get("director"),
                        "producer": properties.get("producer"),
                        "release_date": properties.get("release_date"),
                        "opening_crawl": properties.get("opening_crawl")
                    })
        else:
            # ✅ Handle paginated SWAPI endpoints correctly
            all_entries = fetch_all_pages(path)            
            for entry in all_entries:                
                if "url" in entry:                    
                    detail_data = fetch_data(entry["url"])                    
                    if detail_data and "result" in detail_data:                        
                        properties = detail_data["result"]["properties"]
                        properties["id"] = int(entry["uid"])
                        cleaned_properties = clean_links(properties)  # ✅ Apply link cleaning                    
                        records.append(cleaned_properties)

        df = pd.DataFrame(records)
        print(f"✅ Extracted {len(df)} records from {endpoint}")

        # ✅ Drop rows where `id` is missing
        if not df.empty:
            df = df.dropna(subset=["id"]).astype({"id": "int64"})        

        extracted_data[endpoint] = df        

    return extracted_data  # ✅ Returns a dictionary of DataFrames


if __name__ == "__main__":
    extracted_data = extract_data()
    
    for key, df in extracted_data.items():
        print(f"✅ Extracted {len(df)} records from {key}")
