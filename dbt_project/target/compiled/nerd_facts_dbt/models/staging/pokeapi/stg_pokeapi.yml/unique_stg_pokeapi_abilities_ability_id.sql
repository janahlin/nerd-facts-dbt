
    
    

select
    ability_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_pokeapi_abilities"
where ability_id is not null
group by ability_id
having count(*) > 1


