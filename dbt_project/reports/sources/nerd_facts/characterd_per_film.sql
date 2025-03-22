SELECT
    f.film_title,
    f.episode_id,
    b.film_saga,
    count(b.*) as character_count,
    sum(case when b.film_significance = 'Pivotal'
        then 1
        else 0
    end) as pivotal_characters,
    sum(case when b.character_role = 'Major'
        then 1
        else 0
    end) as major_characters,
    sum(case when b.character_role = 'Supporting'
        then 1
        else 0
    end) as supporting_characters,
    sum(case when b.character_alignment = 'Hero'
        then 1
        else 0
    end) as heroes,
    sum(case when b.character_alignment = 'Villain'
        then 1
        else 0
    end) as villains
FROM
    public.bridge_sw_characters_films b
JOIN
    public.stg_swapi_films f ON b.film_id = f.id
GROUP BY
    f.film_title,
    f.episode_id,
    b.film_saga
ORDER BY
    f.episode_id;