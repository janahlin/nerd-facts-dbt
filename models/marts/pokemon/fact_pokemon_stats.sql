-- Special status
pb.is_legendary,
pb.is_mythical,

-- Generation metrics
CURRENT_TIMESTAMP AS dbt_loaded_at

FROM pokemon_base pb
WHERE pb.pokemon_id IS NOT NULL
ORDER BY pb.total_base_stats DESC 