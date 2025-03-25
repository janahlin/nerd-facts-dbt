/* 
   One Big Table (OBT) for Star Wars Data
   This model combines character, film, planet, and species data into a single denormalized table
   optimized for analytics and reporting.
*/

with characters as (
    select 
        c.character_key,
        c.people_id,
        c.character_name,
        c.height_cm,
        c.mass_kg,
        c.hair_color,
        c.skin_color,
        c.eye_color,
        c.birth_year,
        c.gender,
        c.homeworld_name
    from {{ ref('dim_sw_characters') }} c
),

planets as (
    select
        p.planet_key,
        p.planet_id,
        p.planet_name,
        p.rotation_period,
        p.orbital_period,
        p.diameter,
        p.climate,
        p.gravity,
        p.terrain,
        p.surface_water,
        p.population
    from {{ ref('dim_sw_planets') }} p
),

species as (
    select
        s.species_key,
        s.species_id,
        s.species_name,
        s.classification,
        s.designation,
        s.average_height,
        s.skin_colors,
        s.hair_colors,
        s.eye_colors,
        s.average_lifespan,
        s.language,
        s.homeworld_id
    from {{ ref('dim_sw_species') }} s
),

films as (
    select
        f.film_key,
        f.film_id,
        f.film_title,
        f.episode_id,
        f.opening_crawl,
        f.director,
        f.producer,
        f.release_date
    from {{ ref('dim_sw_films') }} f
),

character_films as (
    select
        cf.character_key,
        cf.film_key
    from {{ ref('bridge_sw_characters_films') }} cf
)

-- Final OBT assembly
select
    -- Character information (core entity)
    c.character_key,
    c.people_id,
    c.character_name,
    c.height_cm,
    c.mass_kg,
    c.hair_color,
    c.skin_color,
    c.eye_color,
    c.birth_year,
    c.gender,
    
    -- Planet information (denormalized)
    p.planet_key,
    p.planet_id,
    p.planet_name,
    p.climate,
    p.terrain,
    p.population,
    
    -- Film information (denormalized)
    f.film_key,
    f.film_id,
    f.film_title,
    f.episode_id,
    f.director,
    f.release_date,
    
    -- Metadata
    current_timestamp as obt_created_at
from characters c
left join planets p on c.homeworld_name = p.planet_name
left join character_films cf on c.character_key = cf.character_key
left join films f on cf.film_key = f.film_key 