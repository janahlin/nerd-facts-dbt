WITH universe_counts AS (
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
)
SELECT
    universe,
    SUM(CASE WHEN entity_type = 'Characters' THEN count ELSE 0 END) as character_count,
    SUM(CASE WHEN entity_type = 'Locations' THEN count ELSE 0 END) as location_count,
    SUM(count) as total_count
FROM
    universe_counts
GROUP BY
    universe
ORDER BY
    total_count DESC;