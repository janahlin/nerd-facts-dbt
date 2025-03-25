-- Query to get a summary of facts across universes
WITH base_counts AS (
    SELECT
        universe,
        COUNT(*) FILTER (WHERE entity_type = 'character') as characters,
        COUNT(*) FILTER (WHERE entity_type = 'location') as locations,
        COUNT(*) as total_entities
    FROM (
        SELECT 'character' as entity_type, universe FROM public.dim_characters
        UNION ALL
        SELECT 'location' as entity_type, universe FROM public.dim_locations
    ) combined
    GROUP BY
        universe
)
SELECT 
    universe,
    'Characters' as entity_type,
    characters as count
FROM base_counts
UNION ALL
SELECT 
    universe,
    'Locations' as entity_type,
    locations as count
FROM base_counts
ORDER BY
    universe, entity_type;