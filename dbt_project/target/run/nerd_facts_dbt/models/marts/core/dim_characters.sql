
  
    

  create  table "nerd_facts"."public"."dim_characters__dbt_tmp"
  
  
    as
  
  (
    /*
  Model: dim_characters (Ultra-simplified)
  Description: Minimal character dimension table
*/

-- Star Wars characters with hardcoded surrogate key inputs
SELECT
    md5(cast(coalesce(cast('star_wars' as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(people_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_key,
    'star_wars' AS universe,
    people_id::TEXT AS character_source_id,  -- Cast to TEXT
    name AS character_name,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM "nerd_facts"."public"."stg_swapi_people"

UNION ALL

-- Pokemon with hardcoded surrogate key inputs
SELECT
    md5(cast(coalesce(cast('pokemon' as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pokemon_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_key,
    'pokemon' AS universe,
    pokemon_id::TEXT AS character_source_id,  -- Cast to TEXT
    pokemon_name AS character_name,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM "nerd_facts"."public"."stg_pokeapi_pokemon"

UNION ALL

-- Netrunner with hardcoded surrogate key inputs
SELECT
    md5(cast(coalesce(cast('netrunner' as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_key,
    'netrunner' AS universe,
    code AS character_source_id,  -- Already TEXT
    card_name AS character_name,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM "nerd_facts"."public"."stg_netrunner_cards"
WHERE type_code = 'identity'
  );
  