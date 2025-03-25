
    
    

select
    move_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_pokeapi_moves"
where move_id is not null
group by move_id
having count(*) > 1


