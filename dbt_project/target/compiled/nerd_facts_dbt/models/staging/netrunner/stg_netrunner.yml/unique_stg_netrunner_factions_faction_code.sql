
    
    

select
    faction_code as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_netrunner_factions"
where faction_code is not null
group by faction_code
having count(*) > 1


