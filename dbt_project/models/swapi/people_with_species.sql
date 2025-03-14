{{ config(materialized='view') }}

SELECT
    p.id,
    p.name,
    s.species_id
FROM {{ ref('stg_swapi_people') }} p
LEFT JOIN {{ ref('swapi_people_species') }} s ON p.id = s.person_id
