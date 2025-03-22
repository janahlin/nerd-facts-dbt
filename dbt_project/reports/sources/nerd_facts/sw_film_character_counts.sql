SELECT
    f.film_title,
    f.episode_id,
    b.film_saga,
    COUNT(*) AS total_characters,
    COUNT(CASE WHEN b.character_role = 'Protagonist/Antagonist' THEN 1 END) AS pivotal_characters,
    COUNT(CASE WHEN b.character_role = 'Major' THEN 1 END) AS major_characters,
    COUNT(CASE WHEN b.character_role = 'Supporting' THEN 1 END) AS supporting_characters,
    COUNT(CASE WHEN b.character_alignment = 'Hero' THEN 1 END) AS heroes,
    COUNT(CASE WHEN b.character_alignment = 'Villain' THEN 1 END) AS villains,
    COUNT(CASE WHEN b.character_alignment = 'Ambiguous' THEN 1 END) AS ambiguous_characters,
    COUNT(CASE WHEN b.character_alignment = 'Neutral' THEN 1 END) AS neutral_characters,
    EXTRACT(YEAR FROM f.release_date) AS release_year
FROM
    public.bridge_sw_characters_films b
JOIN
    public.stg_swapi_films f ON b.film_id = f.id
GROUP BY
    f.film_title,
    f.episode_id,
    b.film_saga,
    EXTRACT(YEAR FROM f.release_date)
ORDER BY
    f.episode_id;