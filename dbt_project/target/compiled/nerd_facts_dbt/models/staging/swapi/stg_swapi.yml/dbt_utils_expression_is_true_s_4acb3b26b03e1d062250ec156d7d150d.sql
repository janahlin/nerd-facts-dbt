

with meet_condition as (
    select * from "nerd_facts"."public"."stg_swapi_starships" where 1=1
)

select
    *
from meet_condition

where not(cargo_capacity >= 0 OR IS NULL)

