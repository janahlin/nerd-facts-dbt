{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM raw.pokeapi_pokemon
)

SELECT 
    id,
    name,
    CAST(height AS NUMERIC) AS height,  -- ✅ Convert to numeric
    CAST(weight AS NUMERIC) AS weight,  -- ✅ Convert to numeric
    ROUND(CAST(weight AS NUMERIC) / (CAST(height AS NUMERIC) * CAST(height AS NUMERIC)), 2) AS bmi
FROM source
