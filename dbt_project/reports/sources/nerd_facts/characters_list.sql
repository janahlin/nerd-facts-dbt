-- Query to get a list of characters with their details
WITH character_data AS (
    SELECT DISTINCT
        character_source_id::text as character_id,
        character_name::text,
        universe::text
    FROM
        public.dim_characters
)
SELECT * FROM character_data
ORDER BY
    universe, character_name
LIMIT 50;