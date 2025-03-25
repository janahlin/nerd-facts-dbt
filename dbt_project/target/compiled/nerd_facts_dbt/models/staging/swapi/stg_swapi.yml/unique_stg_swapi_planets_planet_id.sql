
    
    

select
    planet_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_swapi_planets"
where planet_id is not null
group by planet_id
having count(*) > 1


