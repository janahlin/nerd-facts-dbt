SELECT
    location_key,
    universe,
    location_name,
    location_type,
    gravity,
    climate,
    terrain,
    population
FROM
    public.dim_locations
ORDER BY
    universe,
    location_name;