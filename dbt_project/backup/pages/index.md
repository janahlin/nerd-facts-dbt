# Nerd Facts Dashboard

Welcome to the Nerd Facts visualization dashboard.

## Character Count by Universe

```sql characters_by_universe
SELECT 
  universe, 
  COUNT(*) as character_count
FROM dim_characters  -- Remove 'public.' prefix to let Evidence check all schemas
GROUP BY universe
ORDER BY character_count DESC
```

<BarChart 
  data={characters_by_universe} 
  x="universe" 
  y="character_count" 
  title="Characters by Universe" 
/>

## Power Tiers Distribution

```sql power_distribution
SELECT 
  universe,
  power_tier,
  COUNT(*) as character_count
FROM fct_power_ratings  -- Remove 'public.' prefix
GROUP BY universe, power_tier
ORDER BY universe, power_tier
```

<BarChart 
  data={power_distribution} 
  x="power_tier" 
  y="character_count" 
  series="universe" 
  title="Character Power Distribution" 
  type="grouped" 
/>


