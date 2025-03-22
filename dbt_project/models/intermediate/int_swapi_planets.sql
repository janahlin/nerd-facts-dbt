{{
  config(
    materialized = 'view'
  )
}}

with planets as (    
    select
    planet_id,
    planet_name,
    name,        
    climate,
    gravity,
    terrain,    
    surface_water,
    rotation_period,
    orbital_period,
    diameter,
    population,
    -- Terrain classification flags
    terrain LIKE '%temperate%' AS is_temperate,
    terrain LIKE '%forest%' OR terrain LIKE '%jungle%' OR terrain LIKE '%grassland%' AS has_vegetation,
    terrain LIKE '%ocean%' OR terrain LIKE '%lake%' OR surface_water = '100' AS is_water_world,
    terrain LIKE '%desert%' AS is_desert_world,        
    created_at,
    updated_at,    
    dbt_loaded_at,
    url  
    from {{ ref('stg_swapi_planets') }}
)

select * from planets
