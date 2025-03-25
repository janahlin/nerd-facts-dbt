-- Query to get location counts by universe
SELECT
    universe,
    COUNT(*) as location_count
FROM
    public.dim_locations
GROUP BY
    universe
ORDER BY
    location_count DESC;