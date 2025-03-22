
### Locations Dashboard:

```markdown
# Fictional Locations Dashboard

## Locations by Universe

```sql locations_by_universe
SELECT 
  universe,
  COUNT(*) as location_count,
  AVG(NULLIF(population::NUMERIC, 0)) as avg_population
FROM public.dim_locations
GROUP BY universe

<BarChart data={locations_by_universe} x="universe" y="location_count" title="Location Count by Universe" />