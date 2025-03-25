/*
  Source: obt_universe_distribution
  Description: Distribution of entities across different universes
*/

SELECT
  universe,
  COUNT(*) AS total_entities,
  ROUND(AVG(height_cm)::numeric, 1) AS avg_height_cm,
  ROUND(AVG(weight_kg)::numeric, 1) AS avg_weight_kg,
  COUNT(CASE WHEN gender IS NOT NULL THEN 1 END) AS entities_with_gender,
  COUNT(CASE WHEN species IS NOT NULL THEN 1 END) AS entities_with_species,
  COUNT(CASE WHEN origin_location IS NOT NULL THEN 1 END) AS entities_with_origin
FROM public.nerd_universe_obt
GROUP BY universe
ORDER BY total_entities DESC 