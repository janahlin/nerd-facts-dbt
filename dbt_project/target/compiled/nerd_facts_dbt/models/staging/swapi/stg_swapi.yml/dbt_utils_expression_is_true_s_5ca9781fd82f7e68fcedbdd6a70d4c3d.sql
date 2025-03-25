



select
    1
from "nerd_facts"."public"."stg_swapi_people"

where not(height >= 0 OR IS NULL)

