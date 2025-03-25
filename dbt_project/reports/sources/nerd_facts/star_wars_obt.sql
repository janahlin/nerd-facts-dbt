/*
  Source: star_wars_obt
  Description: Star Wars One Big Table (OBT) containing denormalized character, planet, and film data
*/

SELECT 
  character_key,
  character_name,
  height_cm,
  mass_kg,
  gender,
  hair_color,
  skin_color,
  eye_color,
  birth_year,
  planet_name,
  climate,
  terrain,
  film_title,
  director,
  release_date,
  obt_created_at
FROM public.star_wars_obt
ORDER BY character_name 