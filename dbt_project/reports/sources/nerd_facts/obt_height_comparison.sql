/*
  Source: obt_height_comparison
  Description: Height comparison across universes using the OBT
*/

WITH height_stats AS (
  SELECT
    universe,
    height_category,
    COUNT(*) AS entity_count,
    AVG(height_cm) AS avg_height,
    MIN(height_cm) AS min_height,
    MAX(height_cm) AS max_height
  FROM public.nerd_universe_obt
  WHERE height_cm IS NOT NULL
  GROUP BY universe, height_category
)

SELECT
  universe,
  height_category,
  entity_count,
  ROUND(avg_height::numeric, 1) AS avg_height_cm,
  min_height AS min_height_cm,
  max_height AS max_height_cm
FROM height_stats
ORDER BY universe, 
  CASE 
    WHEN height_category = 'Tall' THEN 1
    WHEN height_category = 'Medium' THEN 2
    WHEN height_category = 'Short' THEN 3
    ELSE 4
  END 