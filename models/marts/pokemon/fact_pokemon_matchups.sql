-- Convert to percentage of total Pokémon
ROUND(pc.pokemon_count * 100.0 / (SELECT COUNT(*) FROM {{ ref('stg_pokeapi_pokemon') }}), 1) AS pct_pokemon_with_type,

-- Date tracking
CURRENT_TIMESTAMP AS dbt_loaded_at

FROM type_matchups tm
JOIN pokemon_types pt ON tm.defender_type = pt.type_name
LEFT JOIN primary_type_counts pc ON tm.defender_type = pc.type_name
ORDER BY tm.attacker_type, tm.defender_type 