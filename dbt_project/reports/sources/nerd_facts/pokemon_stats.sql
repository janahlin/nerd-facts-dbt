-- Query to get Pokémon stats by type
WITH stats_data AS (
    SELECT
        primary_type::text AS primary_type,
        COALESCE(COUNT(pokemon_id)::numeric, 0) AS pokemon_count,
        ROUND(COALESCE(AVG(NULLIF(base_hp, 0))::numeric, 0), 1) AS avg_hp,
        ROUND(COALESCE(AVG(NULLIF(base_attack, 0))::numeric, 0), 1) AS avg_attack,
        ROUND(COALESCE(AVG(NULLIF(base_defense, 0))::numeric, 0), 1) AS avg_defense,
        ROUND(COALESCE(AVG(NULLIF(base_special_attack, 0))::numeric, 0), 1) AS avg_special_attack,
        ROUND(COALESCE(AVG(NULLIF(base_special_defense, 0))::numeric, 0), 1) AS avg_special_defense,
        ROUND(COALESCE(AVG(NULLIF(base_speed, 0))::numeric, 0), 1) AS avg_speed,
        ROUND(COALESCE(AVG(NULLIF(total_base_stats, 0))::numeric, 0), 1) AS avg_total_stats
    FROM
        public.fact_pokemon_stats
    GROUP BY
        primary_type
)
SELECT * FROM stats_data
ORDER BY
    avg_total_stats DESC; 