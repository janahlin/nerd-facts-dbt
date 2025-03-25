---
title: Star Wars Starships Analysis
---

# Star Wars Starships

<small>Data powered by the Star Wars API (SWAPI)</small>

## Starship Classes

```sql starship_classes
SELECT * FROM nerd_facts.starship_classes;
```

<BarChart 
  data={starship_classes} 
  x=starship_class 
  y=ship_count 
  title="Number of Starships by Class" 
/>

<BarChart 
  data={starship_classes} 
  x=starship_class 
  y=avg_length 
  title="Average Length by Starship Class (meters)" 
/>

<ScatterPlot 
  data={starship_classes} 
  x=avg_cost
  y=avg_length
  size=ship_count
  title="Cost vs Size by Starship Class" 
  xAxisTitle="Average Cost (credits)" 
  yAxisTitle="Average Length (meters)" 
  sizeAxisTitle="Number of Ships" 
/>

## Notable Starships

```sql notable_ships
SELECT * FROM nerd_facts.notable_ships;
```

<DataTable 
  data={notable_ships} 
  search=true 
  pagination=true 
  columns={[
    {name: 'starship_id', label: 'ID'},
    {name: 'starship_name', label: 'Name'},
    {name: 'manufacturer', label: 'Manufacturer'},
    {name: 'starship_class', label: 'Class'},
    {name: 'length', label: 'Length (m)'},
    {name: 'max_speed', label: 'Max Speed'},
    {name: 'hyperdrive', label: 'Hyperdrive'},
    {name: 'cost', label: 'Cost'},
    {name: 'film_appearances', label: 'Films'},
    {name: 'film_count', label: 'Film Count'}
  ]} 
/>

<BarChart 
  data={notable_ships} 
  x=starship_name 
  y=length_m 
  title="Size Comparison of Notable Starships (meters)" 
/>

<BarChart 
  data={notable_ships} 
  x=starship_name 
  y=hyperdrive 
  title="Hyperdrive Ratings of Notable Starships (lower is faster)" 
/>

<ScatterPlot 
  data={notable_ships} 
  x=length_m 
  y=cost_credits 
  color=starship_class 
  title="Cost vs Size of Notable Starships" 
  xAxisTitle="Length (meters)" 
  yAxisTitle="Cost (credits)" 
/> 