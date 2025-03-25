



select
    1
from "nerd_facts"."public"."stg_pokeapi_items"

where not(cost >= 0 OR IS NULL)

