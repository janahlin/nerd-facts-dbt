SELECT
    'Characters' as entity_type,
    universe,
    COUNT(*) as count
FROM
    public.dim_characters
GROUP BY
    universe
UNION ALL
SELECT
    'Locations' as entity_type,
    universe,
    COUNT(*) as count
FROM
    public.dim_locations
GROUP BY
    universe
ORDER BY
    entity_type,
    count DESC;