

with starships as (        
    select
        
        starship_id,
        starship_name,
        model,
        manufacturer,
        crew,
        starship_class,
        consumables,        
        cost_in_credits,
        length,
        max_atmosphering_speed,
        cargo_capacity,        
        passengers,
        hyperdrive_rating,
        MGLT,

         -- Entity counts 
        CASE WHEN pilots IS NOT NULL THEN jsonb_array_length(pilots::jsonb) ELSE 0 END AS pilot_count,
        CASE WHEN films IS NOT NULL THEN jsonb_array_length(films::jsonb) ELSE 0 END AS film_appearances,

        -- Ship classification
        CASE
            WHEN lower(starship_class) IN ('corvette', 'frigate', 'star destroyer', 'dreadnought')
                OR lower(starship_name) LIKE '%star destroyer%' THEN 'Military'
            WHEN lower(starship_class) IN ('transport', 'freighter', 'yacht') 
                OR lower(starship_name) LIKE '%transport%' THEN 'Commercial'
            WHEN lower(starship_class) IN ('starfighter', 'bomber', 'assault ship')
                OR lower(starship_name) LIKE '%fighter%' THEN 'Starfighter'
            ELSE 'Other'
        END AS ship_purpose,

        -- Notable ship flag
        CASE
            WHEN starship_name IN ('Millennium Falcon', 'Death Star', 'Star Destroyer', 
                        'X-wing', 'TIE Advanced x1', 'Executor', 'Slave 1') 
            THEN TRUE
            ELSE FALSE
        END AS is_notable_ship,

        created_at,
        edited_at,
        dbt_loaded_at,
        url
    from "nerd_facts"."public"."stg_swapi_starships"
)

select * from starships