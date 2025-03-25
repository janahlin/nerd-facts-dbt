---
title: Cross-Universe Analysis
---

# Cross-Universe Analysis

<small>Comparing metrics across Star Wars and Pokémon universes</small>

## Universe Overview

```sql universe_stats
SELECT 
    COUNT(DISTINCT universe) as total_universes,
    SUM(CASE WHEN entity_type = 'Characters' THEN count ELSE 0 END) as total_characters,
    SUM(CASE WHEN entity_type = 'Locations' THEN count ELSE 0 END) as total_locations
FROM nerd_facts.universe_stats;
```

<div class="grid grid-cols-3 gap-4 mb-8">
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
    value=total_locations 
    title="Total Locations" 
  />
</div>

## Universe Comparison

```sql facts_summary
SELECT * FROM nerd_facts.facts_summary;
```

<div class="grid grid-cols-2 gap-4 mb-8">
  <BarChart 
    data={facts_summary} 
    x=universe 
    y=count 
    title="Total Entities by Universe" 
  />

  <BarChart 
    data={facts_summary} 
    x=universe 
    y=count
    series=entity_type
    type="grouped" 
    title="Characters vs Locations by Universe" 
  />
</div>

## Power Ratings

```sql power_ratings
SELECT * FROM nerd_facts.power_ratings;
```

<div class="grid grid-cols-2 gap-4 mb-8">
  <BarChart 
    data={power_ratings} 
    x=universe 
    y=power_score 
    title="Average Power Rating by Universe" 
  />

  <BarChart 
    data={power_ratings} 
    x=universe 
    y=has_special_abilities    
    type="stacked" 
    title="Special Abilities by Universe" 
  />
</div>

## Character Distribution

```sql characters_by_universe
SELECT * FROM nerd_facts.characters_by_universe;
```

<div class="grid grid-cols-2 gap-4 mb-8">
  <BarChart 
    data={characters_by_universe} 
    x=universe 
    y=character_count 
    title="Character Distribution by Universe" 
  />

  ```sql locations_by_universe
  SELECT * FROM nerd_facts.locations_by_universe;
  ```

  <BarChart 
    data={locations_by_universe} 
    x=universe 
    y=location_count 
    title="Location Distribution by Universe" 
  />
</div>

## Character Details

```sql characters_list
SELECT * FROM nerd_facts.characters_list;
```

<DataTable 
  data={characters_list} 
  search=true 
  pagination=true 
  pageSize=10
/> 
