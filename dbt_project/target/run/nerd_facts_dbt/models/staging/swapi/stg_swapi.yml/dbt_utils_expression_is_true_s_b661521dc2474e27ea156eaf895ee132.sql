select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

with meet_condition as (
    select * from "nerd_facts"."public"."stg_swapi_starships" where 1=1
)

select
    *
from meet_condition

where not(crew::numeric <= passengers::numeric + 50 OR passengers IS NULL OR crew IS NULL OR crew = 'unknown')


      
    ) dbt_internal_test