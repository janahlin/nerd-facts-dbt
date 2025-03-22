SELECT
    universe,
    location_type,
    COUNT(*) as location_count
FROM
    public.dim_locations
WHERE
    location_type IS NOT NULL
GROUP BY
    universe,
    location_type
ORDER BY
    universe,
    location_count DESC;