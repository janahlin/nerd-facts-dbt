
  create view "nerd_facts"."public"."stg_swapi_starships__dbt_tmp"
    
    
  as (
    

WITH raw_data AS (
    SELECT
        id,
        name,
        model,
        manufacturer,
        cost_in_credits,
        length,
        max_atmosphering_speed,
        crew,
        passengers,
        cargo_capacity,
        consumables,
        hyperdrive_rating,
        MGLT,
        starship_class,
        pilots,
        films,
        url,
        created,
        edited
    FROM "nerd_facts"."raw"."swapi_starships"
    WHERE id IS NOT NULL
)

SELECT
    id,
    name AS starship_name,
    model,
    manufacturer,
    starship_class,
    cost_in_credits,
    length AS length_m,
    max_atmosphering_speed AS max_speed,
    hyperdrive_rating,
    MGLT AS mglt,
    crew AS crew_count,
    passengers AS passenger_capacity,
    cargo_capacity,
    consumables,
    
    -- Entity counts 
    CASE WHEN pilots IS NOT NULL THEN jsonb_array_length(pilots::jsonb) ELSE 0 END AS pilot_count,
    CASE WHEN films IS NOT NULL THEN jsonb_array_length(films::jsonb) ELSE 0 END AS film_appearances,
    
    -- Raw arrays
    pilots::jsonb,
    films::jsonb,
    
    -- Ship classification
    CASE
        WHEN lower(starship_class) IN ('corvette', 'frigate', 'star destroyer', 'dreadnought')
            OR lower(name) LIKE '%star destroyer%' THEN 'Military'
        WHEN lower(starship_class) IN ('transport', 'freighter', 'yacht') 
            OR lower(name) LIKE '%transport%' THEN 'Commercial'
        WHEN lower(starship_class) IN ('starfighter', 'bomber', 'assault ship')
            OR lower(name) LIKE '%fighter%' THEN 'Starfighter'
        ELSE 'Other'
    END AS ship_purpose,
    
    -- Size classification (simplified)
    'Unknown' AS ship_size,
    
    -- Notable ship flag
    CASE
        WHEN name IN ('Millennium Falcon', 'Death Star', 'Star Destroyer', 
                     'X-wing', 'TIE Advanced x1', 'Executor', 'Slave 1') 
        THEN TRUE
        ELSE FALSE
    END AS is_notable_ship,
    
    -- Total capacity (simplified)
    0 AS total_capacity,
    
    -- API metadata
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    url,
    
    -- Placeholders
    NULL AS pilot_names,
    NULL AS film_names,
    NULL::TIMESTAMP AS fetch_timestamp,
    NULL::TIMESTAMP AS processed_timestamp,
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
  );