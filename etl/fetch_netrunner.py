import requests
import pandas as pd
import time

BASE_URL = "https://netrunnerdb.com/api/2.0/public/"
ENDPOINTS = {
    "cards": "cards",
    "cycles": "cycles",
    "packs": "packs",
    "prebuilts": "prebuilts",
    "reviews": "reviews",
    "rulings": "rulings",
    "sides": "sides",
    "types": "types",
    "factions": "factions",
    "mwl": "mwl"
}

def fetch_data(url, retries=3, delay=2):
    """Fetch JSON data from API with retries."""
    for attempt in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Attempt {attempt + 1} failed for {url}: {e}")
            time.sleep(delay)  # Wait before retrying
    print(f"❌ Failed to fetch {url} after {retries} attempts")
    return None

def clean_dataframe(df, table_name):
    """Ensure the id column exists and is unique."""
    
    if "id" not in df.columns:
        print(f"⚠️ No 'id' column found in {table_name}, attempting to generate it.")
        df.insert(0, "id", df.index.astype(str))  # Assign index as id

    # Drop rows where id is NULL
    df = df.dropna(subset=["id"])
    
    # Ensure id is unique
    df = df.drop_duplicates(subset=["id"])
    
    return df


def extract_data():
    """
    Extract all datasets from NetrunnerDB API.
    Returns a dictionary of cleaned DataFrames.
    """
    extracted_data = {}

    for endpoint, path in ENDPOINTS.items():
        url = BASE_URL + path
        data = fetch_data(url)

        if not data or "data" not in data:
            print(f"⚠️ No data found for {endpoint}")
            extracted_data[endpoint] = pd.DataFrame()
            continue

        records = data["data"]
        df = pd.DataFrame(records)

        # ✅ Ensure 'id' column exists
        if "id" not in df.columns:
            print(f"⚠️ No 'id' column found in {endpoint}, using 'code' if available.")
            if "code" in df.columns:
                df["id"] = df["code"]
            else:
                df["id"] = df.index.astype(str)  # Fallback to index

        # ✅ Convert 'id' to integer if possible, otherwise create a numeric hash
        try:
            df["id"] = df["id"].astype(int)  # Convert directly if numbers
        except ValueError:
            print(f"⚠️ Non-integer IDs detected in {endpoint}, generating numeric IDs.")
            df["id"] = df["id"].apply(lambda x: abs(hash(x)) % (10**9))  # Create unique numeric ID

        df = df.dropna(subset=["id"])  # Remove rows where id is NULL
        df = df.drop_duplicates(subset=["id"])  # Ensure uniqueness

        extracted_data[endpoint] = df

    return extracted_data




def save_to_csv(data_dict, folder="output/"):
    """Save extracted DataFrames to CSV for debugging."""
    import os
    os.makedirs(folder, exist_ok=True)
    
    for key, df in data_dict.items():
        if not df.empty:
            df.to_csv(f"{folder}{key}.csv", index=False)
            print(f"📁 Saved {key}.csv ({len(df)} rows)")

if __name__ == "__main__":
    extracted_data = extract_data()
    
    for key, df in extracted_data.items():
        print(f"✅ Extracted {len(df)} records from {key}")
    
    save_to_csv(extracted_data)  # Debugging step
