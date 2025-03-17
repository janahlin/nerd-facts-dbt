
    
    

select
    id as unique_field,
    count(*) as n_records

from "pokemon_db"."public"."moves"
where id is not null
group by id
having count(*) > 1


