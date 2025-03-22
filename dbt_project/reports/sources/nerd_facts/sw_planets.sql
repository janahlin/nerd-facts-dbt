SELECT
    location_key,
    location_name,
    location_type,
    diameter,
    rotation_period,
    orbital_period,
    gravity,
    population,
    climate,
    terrain,
    surface_water
FROM
    public.dim_locations
WHERE
    universe = 'star_wars'
ORDER BY
    location_name;