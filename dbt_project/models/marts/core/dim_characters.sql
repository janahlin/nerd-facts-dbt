/*
  Model: dim_characters (Ultra-simplified)
  Description: Minimal character dimension table
*/

-- Star Wars characters with hardcoded surrogate key inputs
SELECT
    {{ dbt_utils.generate_surrogate_key(["'star_wars'", 'id']) }} AS character_key,
    'star_wars' AS universe,
    id::TEXT AS character_source_id,  -- Cast to TEXT
    name AS character_name,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM {{ ref('stg_swapi_people') }}

UNION ALL

-- Pokemon with hardcoded surrogate key inputs
SELECT
    {{ dbt_utils.generate_surrogate_key(["'pokemon'", 'id']) }} AS character_key,
    'pokemon' AS universe,
    id::TEXT AS character_source_id,  -- Cast to TEXT
    name AS character_name,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM {{ ref('stg_pokeapi_pokemon') }}

UNION ALL

-- Netrunner with hardcoded surrogate key inputs
SELECT
    {{ dbt_utils.generate_surrogate_key(["'netrunner'", 'code']) }} AS character_key,
    'netrunner' AS universe,
    code AS character_source_id,  -- Already TEXT
    card_name AS character_name,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM {{ ref('stg_netrunner_cards') }}
WHERE type_name = 'Identity'