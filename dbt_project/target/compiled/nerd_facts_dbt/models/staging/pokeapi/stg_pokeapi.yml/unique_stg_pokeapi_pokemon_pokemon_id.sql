
    
    

select
    pokemon_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_pokeapi_pokemon"
where pokemon_id is not null
group by pokemon_id
having count(*) > 1


