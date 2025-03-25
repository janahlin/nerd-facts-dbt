-- Query to show details about notable Star Wars starships
SELECT
    starship_id,
    starship_name,
    manufacturer,
    starship_class,
    length_m,
    max_speed,
    hyperdrive,
    cost_credits,
    film_appearances,
    film_count
FROM
    public.fact_starships
ORDER BY
    film_count DESC, length_m DESC
LIMIT 20; 