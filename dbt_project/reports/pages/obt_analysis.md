# One Big Table (OBT) Analysis

```sql universe_stats
select * from nerd_facts.obt_universe_distribution
```

## Cross-Universe Analysis

The One Big Table (OBT) pattern enables powerful cross-universe comparisons by denormalizing and standardizing data from different domains. This dashboard showcases various insights derived from our unified nerd facts OBT.

### Universe Distribution

<DataTable data={universe_stats} />

---

### Height Comparison Across Universes

```sql height_stats
select * from nerd_facts.obt_height_comparison
```

<BarChart 
  data={height_stats} 
  x="universe" 
  y="avg_height_cm" 
  series="height_category"
  title="Average Height by Universe and Category"
  yAxisTitle="Height (cm)"
/>

<DataTable data={height_stats} />

---

### Weight Comparison Across Universes

```sql weight_stats
select * from nerd_facts.obt_weight_comparison
```

<BarChart 
  data={weight_stats} 
  x="universe" 
  y="avg_weight_kg" 
  series="weight_category"
  title="Average Weight by Universe and Category"
  yAxisTitle="Weight (kg)"
/>

<DataTable data={weight_stats} />

---

### Largest Entities Across All Universes

```sql largest_entities
select * from nerd_facts.obt_largest_entities
```

<DataTable data={largest_entities} />

---

## Star Wars OBT

The Star Wars OBT combines character, planet, and film data into a single denormalized table for easier analytics.

```sql sw_obt_sample
select * from nerd_facts.star_wars_obt limit 10
```

<DataTable data={sw_obt_sample} />

### Character Height vs. Mass Distribution

```sql sw_height_mass
select 
  character_name, 
  height_cm, 
  mass_kg,
  planet_name
from nerd_facts.star_wars_obt
where height_cm is not null 
  and mass_kg is not null
order by mass_kg desc
```

<ScatterPlot 
  data={sw_height_mass} 
  x="height_cm" 
  y="mass_kg" 
  pointLabel="character_name"
  colorField="planet_name"
  title="Star Wars Character Height vs. Mass"
  xAxisTitle="Height (cm)"
  yAxisTitle="Mass (kg)"
/>

---

## Pokémon OBT

The Pokémon OBT combines Pokémon species, type, and ability data into a single table for easier analytics.

```sql pokemon_obt_sample
select * from nerd_facts.pokemon_obt limit 10
```

<DataTable data={pokemon_obt_sample} />

### Pokémon Stats by Type

```sql pokemon_stats_by_type
select 
  primary_type_name,
  count(*) as pokemon_count,
  round(avg(base_stat_hp)) as avg_hp,
  round(avg(total_base_stats)) as avg_total_stats,
  round(min(total_base_stats)) as min_total_stats,
  round(max(total_base_stats)) as max_total_stats
from nerd_facts.pokemon_obt
group by primary_type_name
order by avg_total_stats desc
```

<BarChart 
  data={pokemon_stats_by_type} 
  x="primary_type_name" 
  y="avg_total_stats" 
  title="Average Total Stats by Primary Type"
  xAxisTitle="Primary Type"
  yAxisTitle="Average Total Stats"
/>

<DataTable data={pokemon_stats_by_type} />

---

## Benefits of the OBT Pattern

The One Big Table (OBT) pattern offers several key benefits:

1. **Simplified Queries**: No complex joins required for multi-entity analysis
2. **Improved Performance**: Pre-joined data leads to faster query performance
3. **Standardized Metrics**: Common attributes across different domains
4. **Cross-Domain Analysis**: Compare entities from different universes easily
5. **Data Exploration**: Easier to explore and discover insights
6. **Reduced Complexity**: Simplified data model for reporting and dashboards

This approach is particularly valuable in analytics scenarios where query performance and ease of use are prioritized over storage efficiency. 