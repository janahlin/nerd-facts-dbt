
    
    

select
    vehicle_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_swapi_vehicles"
where vehicle_id is not null
group by vehicle_id
having count(*) > 1


