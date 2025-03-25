---
title: Pokémon Analysis Dashboard
---

# Pokémon Universe Analysis

<small>Data powered by the PokéAPI</small>

## Pokémon Stats by Type

```sql pokemon_stats
SELECT * FROM nerd_facts.pokemon_stats;
```

<DataTable 
  data={pokemon_stats} 
  search=true 
  pagination=true 
  columns={[
    {name: 'primary_type', label: 'Type'},
    {name: 'pokemon_count', label: 'Count'},
    {name: 'avg_hp', label: 'Avg HP'},
    {name: 'avg_attack', label: 'Avg Attack'},
    {name: 'avg_defense', label: 'Avg Defense'},
    {name: 'avg_special_attack', label: 'Avg Sp. Attack'},
    {name: 'avg_special_defense', label: 'Avg Sp. Defense'},
    {name: 'avg_speed', label: 'Avg Speed'},
    {name: 'avg_total_stats', label: 'Avg Total'}
  ]} 
/>

<BarChart 
  data={pokemon_stats} 
  x=primary_type 
  y=avg_total_stats 
  title="Average Total Stats by Type" 
/>

<BarChart 
  data={pokemon_stats} 
  x=primary_type 
  y=avg_hp 
  series=primary_type
  type="grouped" 
  title="Average HP by Type" 
/>

<BarChart 
  data={pokemon_stats} 
  x=primary_type 
  y=avg_attack 
  series=primary_type
  type="grouped" 
  title="Average Attack by Type" 
/>

<BarChart 
  data={pokemon_stats} 
  x=primary_type 
  y=avg_defense 
  series=primary_type
  type="grouped" 
  title="Average Defense by Type" 
/>

<BarChart 
  data={pokemon_stats} 
  x=primary_type 
  y=avg_special_attack 
  series=primary_type
  type="grouped" 
  title="Average Special Attack by Type" 
/>

<BarChart 
  data={pokemon_stats} 
  x=primary_type 
  y=avg_special_defense 
  series=primary_type
  type="grouped" 
  title="Average Special Defense by Type" 
/>

<BarChart 
  data={pokemon_stats} 
  x=primary_type 
  y=avg_speed 
  series=primary_type
  type="grouped" 
  title="Average Speed by Type" 
/>

<ScatterPlot 
  data={pokemon_stats} 
  x=avg_attack 
  y=avg_defense 
  size=pokemon_count 
  title="Attack vs Defense by Type" 
  xAxisTitle="Average Attack" 
  yAxisTitle="Average Defense" 
  sizeAxisTitle="Number of Pokémon" 
/>

## Top Pokémon

```sql top_pokemon
SELECT * FROM nerd_facts.top_pokemon;
```

<DataTable 
  data={top_pokemon} 
  search=true 
  pagination=true 
  columns={[
    {name: 'pokemon_id', label: 'ID'},
    {name: 'pokemon_name', label: 'Name'},
    {name: 'primary_type', label: 'Primary Type'},
    {name: 'secondary_type', label: 'Secondary Type'},
    {name: 'total_stats', label: 'Total'},
    {name: 'hp', label: 'HP'},
    {name: 'attack', label: 'Attack'},
    {name: 'defense', label: 'Defense'},
    {name: 'special_attack', label: 'Sp.Atk'},
    {name: 'special_defense', label: 'Sp.Def'},
    {name: 'speed', label: 'Speed'},
    {name: 'generation', label: 'Gen'}
  ]} 
/>

<BarChart 
  data={top_pokemon} 
  x=pokemon_name 
  y=hp 
  series=pokemon_name
  type="grouped" 
  title="HP of Top 20 Pokémon" 
/>

<BarChart 
  data={top_pokemon} 
  x=pokemon_name 
  y=attack 
  series=pokemon_name
  type="grouped" 
  title="Attack of Top 20 Pokémon" 
/>

<BarChart 
  data={top_pokemon} 
  x=pokemon_name 
  y=defense 
  series=pokemon_name
  type="grouped" 
  title="Defense of Top 20 Pokémon" 
/>

<BarChart 
  data={top_pokemon} 
  x=pokemon_name 
  y=special_attack 
  series=pokemon_name
  type="grouped" 
  title="Special Attack of Top 20 Pokémon" 
/>

<BarChart 
  data={top_pokemon} 
  x=pokemon_name 
  y=special_defense 
  series=pokemon_name
  type="grouped" 
  title="Special Defense of Top 20 Pokémon" 
/>

<BarChart 
  data={top_pokemon} 
  x=pokemon_name 
  y=speed 
  series=pokemon_name
  type="grouped" 
  title="Speed of Top 20 Pokémon" 
/> 