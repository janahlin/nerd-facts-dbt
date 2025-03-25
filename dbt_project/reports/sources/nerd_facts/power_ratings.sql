SELECT
    universe,
    AVG(normalized_power_score) AS power_score,
    COUNT(CASE WHEN has_special_abilities = 'true' THEN 1 END) AS has_special_abilities,
    COUNT(CASE WHEN has_special_abilities = 'false' THEN 1 END) AS no_special_abilities
FROM
    public.fact_power_ratings
GROUP BY
    universe
ORDER BY
    power_score DESC; 