/*
  Source: pokemon_obt
  Description: Pokémon One Big Table (OBT) containing denormalized Pokémon, type, and ability data
*/

SELECT 
  pokemon_key,
  pokemon_id,
  pokemon_name,
  height_m,
  weight_kg,
  base_experience,
  base_stat_hp,
  total_base_stats,
  primary_type_name,
  secondary_type_name,
  ability_name,
  is_hidden,
  obt_created_at
FROM public.pokemon_obt
ORDER BY pokemon_name 