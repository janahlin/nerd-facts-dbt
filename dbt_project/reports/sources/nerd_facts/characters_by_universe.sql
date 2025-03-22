SELECT
    universe,
    COUNT(*) as character_count
FROM
    public.dim_characters
GROUP BY
    universe
ORDER BY
    character_count DESC;