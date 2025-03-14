import pandas as pd

def clean_pokemon_data(df):
    """Apply data cleaning and transformations to Pokémon dataset."""
    if df.empty:
        print("⚠️ No data to clean, skipping transformations.")
        return df

    # Convert Pokémon names to lowercase
    df["name"] = df["name"].str.lower()

    # Remove duplicates
    df = df.drop_duplicates()

    # Fill missing values with defaults
    df["height"] = df["height"].fillna(0)
    df["weight"] = df["weight"].fillna(0)

    # Add a new column for BMI (Body Mass Index)
    df["bmi"] = round(df["weight"] / (df["height"] ** 2), 2)

    print(f"✅ Transformed {len(df)} records.")
    return df

if __name__ == "__main__":
    # Example usage with test data
    sample_data = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Pikachu", "Charizard", "Bulbasaur", "Pikachu"],
        "height": [4, 17, 7, 4],
        "weight": [60, 905, 69, 60]
    })

    cleaned_df = clean_pokemon_data(sample_data)
    print(cleaned_df)

