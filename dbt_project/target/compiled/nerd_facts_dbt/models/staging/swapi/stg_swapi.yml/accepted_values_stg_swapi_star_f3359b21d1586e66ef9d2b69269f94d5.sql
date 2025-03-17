
    
    

with all_values as (

    select
        manufacturer as value_field,
        count(*) as n_records

    from "nerd_facts"."public"."stg_swapi_starships"
    group by manufacturer

)

select *
from all_values
where value_field not in (
    'Kuat Drive Yards','Corellian Engineering Corporation','Sienar Fleet Systems','Cygnus Spaceworks','Incom Corporation','Koensayr Manufacturing'
)


