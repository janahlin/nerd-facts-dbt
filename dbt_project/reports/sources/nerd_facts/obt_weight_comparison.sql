/*
  Source: obt_weight_comparison
  Description: Weight comparison across universes using the OBT
*/

WITH weight_stats AS (
  SELECT
    universe,
    weight_category,
    COUNT(*) AS entity_count,
    AVG(weight_kg) AS avg_weight,
    MIN(weight_kg) AS min_weight,
    MAX(weight_kg) AS max_weight
  FROM public.nerd_universe_obt
  WHERE weight_kg IS NOT NULL
  GROUP BY universe, weight_category
)

SELECT
  universe,
  weight_category,
  entity_count,
  ROUND(avg_weight::numeric, 1) AS avg_weight_kg,
  min_weight AS min_weight_kg,
  max_weight AS max_weight_kg
FROM weight_stats
ORDER BY universe, 
  CASE 
    WHEN weight_category = 'Heavy' THEN 1
    WHEN weight_category = 'Medium' THEN 2
    WHEN weight_category = 'Light' THEN 3
    ELSE 4
  END 