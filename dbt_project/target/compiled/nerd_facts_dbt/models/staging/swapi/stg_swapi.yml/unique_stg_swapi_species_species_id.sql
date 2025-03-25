
    
    

select
    species_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_swapi_species"
where species_id is not null
group by species_id
having count(*) > 1


