-- Query to get character counts by universe
WITH character_counts AS (
    SELECT
        universe,
        COUNT(*)::numeric as character_count
    FROM
        public.dim_characters
    GROUP BY
        universe
)
SELECT 
    universe,
    character_count
FROM character_counts
ORDER BY
    character_count DESC;