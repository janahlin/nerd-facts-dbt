
    
    

select
    code as unique_field,
    count(*) as n_records

from "nerd_facts"."raw"."netrunner_packs"
where code is not null
group by code
having count(*) > 1


