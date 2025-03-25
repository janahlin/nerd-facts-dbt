/* 
   One Big Table (OBT) for Pokémon Data
   This model combines pokemon, types, abilities, and stats into a single denormalized table
   optimized for analytics and reporting.
*/

with pokemon as (
    select 
        p.pokemon_key,
        p.pokemon_id,
        p.pokemon_name,
        p.height_m,
        p.weight_kg,
        p.base_xp as base_experience,
        p.primary_type,
        p.secondary_type,
        p.primary_type_key,
        p.secondary_type_key,
        p.base_stat_hp,
        p.total_base_stats
    from {{ ref('fact_pokemon') }} p
),

types as (
    select
        t.type_key,
        t.type_id,
        t.type_name
    from {{ ref('dim_pokemon_types') }} t
),

pokemon_abilities as (
    select
        pa.pokemon_ability_id,
        pa.pokemon_id,
        pa.ability_name,
        pa.is_hidden,
        pa.slot_number
    from {{ ref('bridge_pokemon_abilities') }} pa
)

-- Final OBT assembly
select
    -- Pokemon information (core entity)
    p.pokemon_key,
    p.pokemon_id,
    p.pokemon_name,
    p.height_m,
    p.weight_kg,
    p.base_experience,
    p.base_stat_hp,
    p.total_base_stats,
    
    -- Type information (denormalized)
    p.primary_type as primary_type_name,
    p.secondary_type as secondary_type_name,
    
    -- Ability information (denormalized)
    pa.ability_name,
    pa.is_hidden,
    pa.slot_number,
    
    -- Metadata
    current_timestamp as obt_created_at
from pokemon p
left join pokemon_abilities pa on p.pokemon_id = pa.pokemon_id 