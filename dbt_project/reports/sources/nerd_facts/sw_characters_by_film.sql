SELECT
    f.film_title,
    f.episode_id,
    b.film_saga,
    b.character_name,
    b.character_role,
    b.narrative_role,
    b.character_alignment,
    b.film_significance,
    p.height_cm,
    p.mass_kg,
    p.hair_color,
    p.skin_color,
    p.eye_color,
    p.gender,
    p.homeworld_id
FROM
    public.bridge_sw_characters_films b
JOIN
    public.stg_swapi_films f ON b.film_id = f.id
JOIN
    public.stg_swapi_people p ON b.character_id = p.id
ORDER BY
    f.episode_id,
    CASE
        WHEN b.film_significance = 'Pivotal' THEN 1
        WHEN b.film_significance = 'Crucial' THEN 2
        WHEN b.film_significance = 'Significant' THEN 3
        WHEN b.film_significance = 'Important' THEN 4
        ELSE 5
    END,
    b.character_name;