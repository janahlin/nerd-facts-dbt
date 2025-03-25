
    
    

select
    people_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_swapi_people"
where people_id is not null
group by people_id
having count(*) > 1


