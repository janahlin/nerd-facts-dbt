---
title: Star Wars Universe Dashboard
---

# Star Wars Universe

<small>Data powered by the Star Wars API (SWAPI)</small>

## Films Timeline

```sql sw_films
SELECT 
  episode_id,
  film_title,
  director,
  release_date
FROM
  nerd_facts.sw_films
ORDER BY episode_id;
```

<DataTable data={sw_films} columns={[ {name: 'episode_id', label: 'Episode'}, {name: 'film_title', label: 'Title'}, {name: 'director', label: 'Director'}, {name: 'release_date', label: 'Release Date'}, ]} />

## Characters per film

```sql characters_per_film
SELECT 
    film_title,
    episode_id
    film_saga,
    character_count,
    pivotal_characters,
    major_characters,
    supporting_characters,
    heroes,
    villains    
FROM 
  nerd_facts.characterd_per_film;
```

<BarChart data={characters_per_film} x=film_title y=character_count title="Total Characters per Film" />

<BarChart data={characters_per_film} x=film_title series={['heroes', 'villains']} type="grouped" title="Heroes vs Villains by Film" />