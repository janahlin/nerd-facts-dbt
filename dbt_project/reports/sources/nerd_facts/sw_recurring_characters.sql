WITH character_appearances AS (
    SELECT
        b.character_id,
        b.character_name,
        COUNT(DISTINCT b.film_id) AS film_appearances,
        b.saga_appearance_percentage,
        string_agg(f.film_title, ', ' ORDER BY f.episode_id) AS appeared_in_films,
        CASE
            WHEN MAX(b.character_role) = 'Protagonist/Antagonist' THEN 'Lead Character'
            WHEN MAX(b.character_role) = 'Major' THEN 'Major Character'
            ELSE 'Supporting Character'
        END AS overall_role,
        MAX(b.character_alignment) AS alignment
    FROM
        public.bridge_sw_characters_films b
    JOIN
        public.stg_swapi_films f ON b.film_id = f.id
    GROUP BY
        b.character_id,
        b.character_name,
        b.saga_appearance_percentage
)
SELECT
    ca.character_name,
    ca.film_appearances,
    ca.saga_appearance_percentage,
    ca.appeared_in_films,
    ca.overall_role,
    ca.alignment,
    p.height_cm,
    p.mass_kg,
    p.gender,
    p.homeworld_id
FROM
    character_appearances ca
JOIN
    public.stg_swapi_people p ON ca.character_id = p.id
WHERE
    ca.film_appearances > 1  -- Only characters in multiple films
ORDER BY
    ca.film_appearances DESC,
    ca.saga_appearance_percentage DESC,
    ca.character_name;