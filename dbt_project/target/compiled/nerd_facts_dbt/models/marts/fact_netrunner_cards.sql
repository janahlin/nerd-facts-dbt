WITH cards AS (
    SELECT * FROM "nerd_facts"."public"."stg_netrunner"
)
SELECT
    c.id,
    c.card_name,
    c.faction_name,  -- ✅ Already included in stg_netrunner
    c.type_name,  -- ✅ Now available in stg_netrunner
    c.cost,
    c.description
FROM cards c