

WITH raw_data AS (
    SELECT
        -- Id fields
        id,

        -- Text fields
        name,
        model,
        manufacturer,
        crew,
        starship_class,
        consumables,
        url,

        -- Numeric fields
        CASE cost_in_credits~E'[0-9]+$' WHEN TRUE THEN cost_in_credits ELSE NULL END AS cost_in_credits,
        CASE length~E'[0-9]+$' WHEN TRUE THEN length ELSE NULL END AS length,
        CASE TRIM(TRAILING 'km' from max_atmosphering_speed)~E'[0-9]+$' WHEN TRUE THEN TRIM(TRAILING 'km' from max_atmosphering_speed) ELSE NULL END AS max_atmosphering_speed,
        CASE cargo_capacity~E'[0-9]+$' WHEN TRUE THEN cargo_capacity ELSE NULL END AS cargo_capacity,        
        CASE passengers~E'[0-9]+$' WHEN TRUE THEN passengers ELSE NULL END AS passengers,
        CASE hyperdrive_rating~E'[0-9]+$' WHEN TRUE THEN hyperdrive_rating ELSE NULL END AS hyperdrive_rating,
        CASE MGLT~E'[0-9]+$' WHEN TRUE THEN MGLT ELSE NULL END AS MGLT,        
        
        -- Relationship arrays
        pilots,
        films,

        -- Timestamp fields
        created,
        edited
    FROM "nerd_facts"."raw"."swapi_starships"
    WHERE id IS NOT NULL
)

SELECT
      -- Id fields
        id as starship_id,

        -- Text fields
        name AS starship_name,
        model,
        manufacturer,
        crew,
        starship_class,
        consumables,
        url,

        -- Numeric fields
        CAST(cost_in_credits AS NUMERIC) AS cost_in_credits,
        CAST(REPLACE(length, ',', '') AS NUMERIC) AS length,
        CAST(max_atmosphering_speed AS NUMERIC) AS max_atmosphering_speed,
        CAST(cargo_capacity AS NUMERIC) AS cargo_capacity,        
        CAST(REPLACE(passengers, ',', '') AS NUMERIC) AS passengers,
        CAST(hyperdrive_rating AS NUMERIC) AS hyperdrive_rating,
        CAST(MGLT AS NUMERIC) AS MGLT,        
        
        -- Relationship arrays
        pilots,
        films,

        -- Timestamp fields
        CAST(created AS TIMESTAMP) AS created_at,
        CAST(edited AS TIMESTAMP) AS edited_at,

        -- Data tracking field
        CURRENT_TIMESTAMP AS dbt_loaded_at 
    
FROM raw_data