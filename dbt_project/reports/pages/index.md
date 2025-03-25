---
title: Nerd Facts Dashboard
---

# Nerd Facts Dashboard

<small>Data powered by various APIs</small>

## Universe Stats

```sql universe_stats
SELECT 
    COUNT(DISTINCT universe) as total_universes,
    SUM(CASE WHEN entity_type = 'Characters' THEN count ELSE 0 END) as total_characters,
    SUM(CASE WHEN entity_type = 'Locations' THEN count ELSE 0 END) as total_locations
FROM nerd_facts.universe_stats;
```

<BigValue 
  data={universe_stats} 
  value=total_universes 
  title="Total Universes" 
/>

<BigValue 
  data={universe_stats} 
  value=total_characters 
  title="Total Characters" 
/>

<BigValue 
  data={universe_stats} 
  title="Total Locations" 
  value=total_locations 
/>

## Available Dashboards

- [Star Wars Universe](star_wars_dashboard)
- [Star Wars Starships](star_wars_starships)
- [Pokémon Analysis](pokemon_dashboard)
- [Cross-Universe Comparison](universe_comparison)

## Character Distribution

```sql character_distribution
SELECT * FROM nerd_facts.character_distribution;
```

<BarChart 
  data={character_distribution} 
  x=universe 
  y=character_count 
  title="Character Distribution by Universe" 
/>

## Total Records by Universe

```sql recent_updates
SELECT * FROM nerd_facts.recent_updates;
```

<BarChart 
  data={recent_updates} 
  x=universe 
  y=total_count 
  title="Total Records by Universe" 
/>
