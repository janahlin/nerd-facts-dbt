-- Query to compare character power ratings across universes
SELECT
    universe,
    AVG(normalized_power_score) AS avg_power_score,
    COUNT(*) AS character_count,
    COUNT(CASE WHEN has_special_abilities = 'true' THEN 1 END) AS with_special_abilities,
    COUNT(CASE WHEN has_special_abilities = 'false' THEN 1 END) AS without_special_abilities
FROM
    public.fact_power_ratings
GROUP BY
    universe
ORDER BY
    avg_power_score DESC; 