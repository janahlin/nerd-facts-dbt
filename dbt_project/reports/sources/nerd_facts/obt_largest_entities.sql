/*
  Source: obt_largest_entities
  Description: Tallest and heaviest entities across all universes
*/

SELECT
  universe,
  entity_name,
  entity_type,
  height_cm,
  weight_kg,
  height_category,
  weight_category,
  species,
  origin_location
FROM public.nerd_universe_obt
WHERE height_cm IS NOT NULL OR weight_kg IS NOT NULL
ORDER BY 
  CASE WHEN height_cm IS NULL THEN 0 ELSE height_cm END DESC,
  CASE WHEN weight_kg IS NULL THEN 0 ELSE weight_kg END DESC
LIMIT 20 