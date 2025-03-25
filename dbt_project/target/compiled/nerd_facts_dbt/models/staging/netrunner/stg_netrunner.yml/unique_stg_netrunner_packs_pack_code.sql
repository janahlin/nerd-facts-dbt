
    
    

select
    pack_code as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_netrunner_packs"
where pack_code is not null
group by pack_code
having count(*) > 1


