-- Query to get overall universe statistics
WITH base_data AS (
    SELECT
        universe,
        entity_type,
        COUNT(*) as count
    FROM (
        SELECT 'Characters' as entity_type, universe FROM public.dim_characters
        UNION ALL
        SELECT 'Locations' as entity_type, universe FROM public.dim_locations
    ) combined
    GROUP BY
        universe,
        entity_type
)
SELECT 
    universe,
    entity_type,
    count
FROM base_data;