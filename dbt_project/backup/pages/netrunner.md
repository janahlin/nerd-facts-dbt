# Netrunner Cards Analysis

## Card Power by Faction

```sql faction_power
SELECT 
  faction_code,
  AVG(normalized_power_score) as avg_power,
  COUNT(*) as card_count
FROM public.fct_netrunner_card_power
GROUP BY faction_code
ORDER BY avg_power DESC
```

<ScatterPlot 
  data={faction_power} 
  x="faction_code" 
  y="avg_power" 
  size="card_count" 
  title="Average Card Power by Faction" 
/>

## Card Type Distribution

```sql card_types
SELECT 
  type_name, 
  COUNT(*) as card_count
FROM public.stg_netrunner_cards
GROUP BY type_name
ORDER BY card_count DESC
```

<PieChart
  data={card_types}
  value="card_count"
  category="type_name"
  title="Card Types Distribution"
/>