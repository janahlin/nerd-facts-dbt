import os

# Get the absolute path of the script's directory (etl/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the base directory of your dbt project relative to the script's location
DBT_PROJECT_DIR = os.path.join(SCRIPT_DIR, "../dbt_project/models")

# Define model files and their contents
dbt_files = {
    "staging/stg_pokeapi.sql": """WITH raw_data AS (
    SELECT * FROM raw.pokeapi_pokemon
)
SELECT
    id,
    name AS pokemon_name,
    height / 10.0 AS height_m,
    weight / 10.0 AS weight_kg,
    base_experience,
    jsonb_array_length(abilities) AS num_abilities,
    abilities AS abilities_json,
    types->0->>'type' AS primary_type
FROM raw_data""",

    "staging/stg_swapi.sql": """WITH raw_data AS (
    SELECT * FROM raw.swapi_starships
)
SELECT
    id,
    name AS starship_name,
    model,
    manufacturer,
    CASE 
        WHEN max_atmosphering_speed ~ '^[0-9]+$' THEN max_atmosphering_speed::NUMERIC
        ELSE NULL 
    END AS max_speed,
    crew::INTEGER,
    passengers::INTEGER
FROM raw_data""",

    "staging/stg_netrunner.sql": """WITH raw_data AS (
    SELECT * FROM raw.netrunner_cards
)
SELECT
    id,
    title AS card_name,
    faction_code,
    (SELECT name FROM raw.netrunner_factions f WHERE f.code = raw_data.faction_code) AS faction_name,
    type_code,
    (SELECT name FROM raw.netrunner_types t WHERE t.code = raw_data.type_code) AS type_name,
    cost::INTEGER,
    text AS description
FROM raw_data""",

    "marts/fact_pokemon.sql": """WITH pokemon AS (
    SELECT * FROM {{ ref('stg_pokeapi') }}
)
SELECT
    id,
    pokemon_name,
    height_m,
    weight_kg,
    num_abilities,
    primary_type
FROM pokemon""",

    "marts/fact_starships.sql": """WITH starships AS (
    SELECT * FROM {{ ref('stg_swapi') }}
)
SELECT
    id,
    starship_name,
    model,
    manufacturer,
    max_speed,
    crew,
    passengers
FROM starships""",

    "marts/fact_netrunner_cards.sql": """WITH cards AS (
    SELECT * FROM {{ ref('stg_netrunner') }}
)
SELECT
    id,
    card_name,
    faction_name,
    type_name,
    cost,
    description
FROM cards""",

    "marts/dim_pokemon_types.sql": """WITH types AS (
    SELECT DISTINCT
        primary_type,
        COUNT(*) AS num_pokemon
    FROM {{ ref('stg_pokeapi') }}
    GROUP BY primary_type
)
SELECT * FROM types""",

    "marts/dim_starship_manufacturers.sql": """WITH manufacturers AS (
    SELECT DISTINCT
        manufacturer,
        COUNT(*) AS num_starships
    FROM {{ ref('stg_swapi') }}
    GROUP BY manufacturer
)
SELECT * FROM manufacturers""",

    "marts/dim_netrunner_factions.sql": """WITH factions AS (
    SELECT DISTINCT
        faction_name,
        COUNT(*) AS num_cards
    FROM {{ ref('stg_netrunner') }}
    GROUP BY faction_name
)
SELECT * FROM factions"""
}

# Function to create files
def create_dbt_files():
    for path, content in dbt_files.items():
        file_path = os.path.join(DBT_PROJECT_DIR, path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"✅ Created: {file_path}")

# Run the function
if __name__ == "__main__":
    create_dbt_files()
    print("\n🎉 All dbt models have been created successfully!")
