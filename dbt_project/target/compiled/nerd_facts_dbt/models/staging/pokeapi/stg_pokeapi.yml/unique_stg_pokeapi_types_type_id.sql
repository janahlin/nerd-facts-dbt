
    
    

select
    type_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_pokeapi_types"
where type_id is not null
group by type_id
having count(*) > 1


