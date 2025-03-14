

SELECT
    p.id,
    p.name,
    s.species_id
FROM "nerd_facts"."public"."stg_swapi_people" p
LEFT JOIN "nerd_facts"."public"."swapi_people_species" s ON p.id = s.person_id