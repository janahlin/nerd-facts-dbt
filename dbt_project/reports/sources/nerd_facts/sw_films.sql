SELECT
    sf.film_title,
    sf.episode_id,
    sf.director,
    sf.producer,
    sf.release_date,
    sf.opening_crawl
FROM
    public.stg_swapi_films sf
ORDER BY
    sf.episode_id;