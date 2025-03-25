
    
    

select
    faction_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_netrunner_factions"
where faction_id is not null
group by faction_id
having count(*) > 1


