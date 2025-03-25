
    
    

select
    card_id as unique_field,
    count(*) as n_records

from "nerd_facts"."public"."stg_netrunner_cards"
where card_id is not null
group by card_id
having count(*) > 1


