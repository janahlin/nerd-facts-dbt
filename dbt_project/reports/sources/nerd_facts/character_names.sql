SELECT
    universe,
    LENGTH(character_name) as name_length,
    COUNT(*) as character_count
FROM
    public.dim_characters
GROUP BY
    universe,
    LENGTH(character_name)
ORDER BY
    universe,
    name_length;