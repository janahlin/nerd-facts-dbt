
    
    

with all_values as (

    select
        side_code as value_field,
        count(*) as n_records

    from "nerd_facts"."public"."stg_netrunner_cards"
    group by side_code

)

select *
from all_values
where value_field not in (
    'corp','runner'
)


