-- Query to get total counts by universe
-- TODO: Future improvement - Add updated_at columns to track when records were last modified
-- This would allow us to show actual recent updates instead of just total counts
SELECT 
    'Star Wars' as universe,
    COUNT(*) as total_count
FROM 
    public.dim_sw_films
UNION ALL
SELECT 
    'Pokémon' as universe,
    COUNT(*) as total_count
FROM 
    public.fact_pokemon
ORDER BY 
    universe; 