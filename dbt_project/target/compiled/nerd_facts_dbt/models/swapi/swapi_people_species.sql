

WITH species_data AS (
    SELECT
        id AS species_id,
        jsonb_array_elements_text(people::JSONB)::INTEGER AS person_id  -- ✅ Cast TEXT to JSONB
    FROM "nerd_facts"."raw"."swapi_species"
)

SELECT
    person_id,
    species_id
FROM species_data
WHERE person_id IS NOT NULL