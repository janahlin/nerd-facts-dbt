---
title: Star Wars Universe Dashboard
---

# Star Wars Dashboard

<small>Data powered by the Star Wars API (SWAPI)</small>

## Films Timeline

```sql sw_films
select 
  episode_id,
  film_title,
  director,
  release_date
from
  nerd_facts.sw_films
order by episode_id
```

<DataTable data={sw_films} columns={[ {name: 'episode_id', label: 'Episode'}, {name: 'film_title', label: 'Title'}, {name: 'director', label: 'Director'}, {name: 'release_date', label: 'Release Date'}, ]} />

## Characters per film

```sql characters_per_film
select 
    film_title,
    episode_id,
    film_saga,
    character_count,
    pivotal_characters,
    major_characters,
    supporting_characters,
    heroes,
    villains    
from 
    nerd_facts.characters_per_film
```

<BarChart data={characters_per_film} x="film_title" y="character_count" title="Total Characters per Film" />

<BarChart 
  data={characters_per_film} 
  x="film_title" 
  y="heroes" 
  title="Heroes by Film" 
/>

<BarChart 
  data={characters_per_film} 
  x="film_title" 
  y="villains" 
  title="Villains by Film" 
/>

## Character Distribution

```sql character_distribution
select * from nerd_facts.character_distribution
```

<BarChart 
  data={character_distribution} 
  x="universe" 
  y="character_count"
  title="Character Count by Universe"
/>