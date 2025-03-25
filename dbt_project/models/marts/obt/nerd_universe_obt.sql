/* 
   Master One Big Table (OBT) for Nerd Universes Data
   This model combines multiple fictional universes into a single unified table
   with a common structure for cross-universe analysis.
*/

with star_wars_characters as (
    select 
        'Star Wars' as universe,
        c.character_key as entity_key,
        c.people_id as entity_id,
        c.character_name as entity_name,
        'character' as entity_type,
        c.gender,
        c.height_cm,
        c.mass_kg as weight_kg,
        c.homeworld_name as origin_location,
        NULL as species,
        NULL as abilities,
        c.film_appearances as appears_in,
        NULL as first_appearance_date,
        'human' as creator_type
    from {{ ref('dim_sw_characters') }} c
),

pokemon_entities as (
    select
        'Pokemon' as universe,
        p.pokemon_key as entity_key,
        p.pokemon_id as entity_id,
        p.pokemon_name as entity_name,
        'pokemon' as entity_type,
        null as gender,
        p.height_m * 100 as height_cm,  -- convert to cm
        p.weight_kg,
        null as origin_location,
        p.primary_type as species,
        null as abilities,
        null as appears_in,
        null as first_appearance_date,
        'digital' as creator_type
    from {{ ref('fact_pokemon') }} p
)

-- Final Master OBT
select
    universe,
    entity_key,
    entity_id,
    entity_name,
    entity_type,
    gender,
    height_cm,
    weight_kg,
    origin_location,
    species,
    abilities,
    appears_in,
    first_appearance_date,
    creator_type,
    -- Add computed/derived columns
    case
        when height_cm > 200 then 'Tall'
        when height_cm between 100 and 200 then 'Medium'
        when height_cm < 100 then 'Short'
        else 'Unknown'
    end as height_category,
    case
        when weight_kg > 100 then 'Heavy'
        when weight_kg between 50 and 100 then 'Medium'
        when weight_kg < 50 then 'Light'
        else 'Unknown'
    end as weight_category,
    current_timestamp as obt_created_at
from (
    select * from star_wars_characters
    union all
    select * from pokemon_entities
) as unified_entities 