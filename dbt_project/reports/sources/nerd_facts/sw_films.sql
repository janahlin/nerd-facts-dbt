-- Query to get Star Wars films timeline
SELECT 
    f.episode_id,
    f.film_title,
    f.director,
    f.release_date
FROM 
    public.dim_sw_films f
ORDER BY 
    f.episode_id; 