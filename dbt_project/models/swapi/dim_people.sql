{{ config(materialized='view') }}

SELECT
    id,
    name,
    -- ✅ Convert `height` safely
    CASE 
        WHEN height ~ '^\d+$' THEN height::DECIMAL
        ELSE NULL
    END AS height,
    -- ✅ Convert `mass` safely
    CASE 
        WHEN mass ~ '^\d+$' THEN mass::DECIMAL
        ELSE NULL
    END AS mass,
    hair_color,
    skin_color,
    eye_color,
    birth_year,
    gender,
    homeworld::INTEGER AS homeworld_id,
    url
FROM {{ source('raw', 'swapi_people') }}
