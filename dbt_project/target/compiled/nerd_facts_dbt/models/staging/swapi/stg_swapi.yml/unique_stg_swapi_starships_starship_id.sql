
    
    

select
    starship_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_swapi_starships"
where starship_id is not null
group by starship_id
having count(*) > 1


