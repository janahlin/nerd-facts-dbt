SELECT
    starship_class,
    COUNT(*) AS ship_count,
    AVG(length_m) AS avg_length,
    AVG(cost_credits) AS avg_cost,
    AVG(film_count) AS avg_film_appearances
FROM
    public.fact_starships
GROUP BY
    starship_class
ORDER BY
    ship_count DESC; 