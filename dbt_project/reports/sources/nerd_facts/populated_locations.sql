SELECT
    universe,
    location_name,
    location_type,
    CASE
        WHEN population ~ '^[0-9]+$' THEN population::bigint
        ELSE NULL
    END as population_count
FROM
    public.dim_locations
WHERE
    population IS NOT NULL
    AND population != 'unknown'
    AND population ~ '^[0-9]+$'
ORDER BY
    (population::bigint) DESC NULLS LAST
LIMIT 15;