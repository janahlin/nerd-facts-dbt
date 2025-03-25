-- Query to get top Pokémon by stats
WITH pokemon_stats AS (
  SELECT 
    p.pokemon_id,
    p.pokemon_name,
    p.primary_type::text,
    p.secondary_type::text,
    p.generation_number as generation,
    s.base_hp as hp,
    s.base_attack as attack,
    s.base_defense as defense,
    s.base_special_attack as special_attack,
    s.base_special_defense as special_defense,
    s.base_speed as speed,
    s.total_base_stats as total_stats
  FROM public.fact_pokemon p
  JOIN public.fact_pokemon_stats s ON p.pokemon_id = s.pokemon_id
)
SELECT * FROM pokemon_stats
ORDER BY total_stats DESC
LIMIT 20; 