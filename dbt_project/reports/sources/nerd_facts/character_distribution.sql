-- Query to get character distribution across universes
SELECT 
    'Star Wars' as universe,
    COUNT(*) as character_count
FROM 
    public.dim_characters c
    JOIN public.bridge_sw_characters_films bf ON c.character_source_id = bf.character_id::text
UNION ALL
SELECT 
    'Pokémon' as universe,
    COUNT(*) as character_count
FROM 
    public.fact_pokemon
ORDER BY 
    universe; 