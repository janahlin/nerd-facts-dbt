
    
    

select
    id as unique_field,
    count(*) as n_records

from "nerd_facts"."raw"."swapi_vehicles"
where id is not null
group by id
having count(*) > 1


