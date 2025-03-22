SELECT
    character_key,
    universe,
    character_name,
    character_source_id
FROM
    public.dim_characters
ORDER BY
    universe,
    character_name;