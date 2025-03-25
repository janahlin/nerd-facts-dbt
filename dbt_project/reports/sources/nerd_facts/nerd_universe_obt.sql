/*
  Source: nerd_universe_obt
  Description: Cross-universe One Big Table (OBT) containing unified data from Star Wars and Pokémon universes
*/

SELECT 
  universe,
  entity_key,
  entity_id,
  entity_name,
  entity_type,
  gender,
  height_cm,
  weight_kg,
  origin_location,
  species,
  abilities,
  appears_in,
  creator_type,
  height_category,
  weight_category,
  obt_created_at
FROM public.nerd_universe_obt
ORDER BY universe, entity_name 