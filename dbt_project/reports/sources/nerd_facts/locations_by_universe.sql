SELECT
    universe,
    COUNT(*) as location_count,
    array_agg(DISTINCT location_type) as location_types
FROM
    public.dim_locations
GROUP BY
    universe
ORDER BY
    location_count DESC;