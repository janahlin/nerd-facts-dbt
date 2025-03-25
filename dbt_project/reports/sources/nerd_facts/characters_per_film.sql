-- Query to get character statistics per film
WITH film_characters AS (
    SELECT 
        f.episode_id,
        f.film_title,
        f.film_saga,
        COALESCE(COUNT(DISTINCT c.character_source_id), 0) as character_count,
        COALESCE(COUNT(DISTINCT CASE WHEN bf.film_significance = 'pivotal' THEN c.character_source_id END), 0) as pivotal_characters,
        COALESCE(COUNT(DISTINCT CASE WHEN bf.film_significance = 'major' THEN c.character_source_id END), 0) as major_characters,
        COALESCE(COUNT(DISTINCT CASE WHEN bf.film_significance = 'supporting' THEN c.character_source_id END), 0) as supporting_characters,
        COALESCE(COUNT(DISTINCT CASE WHEN bf.character_alignment = 'hero' THEN c.character_source_id END), 0) as heroes,
        COALESCE(COUNT(DISTINCT CASE WHEN bf.character_alignment = 'villain' THEN c.character_source_id END), 0) as villains
    FROM 
        public.dim_sw_films f
        LEFT JOIN public.bridge_sw_characters_films bf ON f.episode_id = bf.episode_id
        LEFT JOIN public.dim_characters c ON bf.character_id::text = c.character_source_id
    GROUP BY 
        f.episode_id,
        f.film_title,
        f.film_saga
)
SELECT 
    episode_id,
    film_title,
    film_saga,
    character_count::numeric,
    pivotal_characters::numeric,
    major_characters::numeric,
    supporting_characters::numeric,
    heroes::numeric,
    villains::numeric
FROM film_characters
ORDER BY episode_id; 