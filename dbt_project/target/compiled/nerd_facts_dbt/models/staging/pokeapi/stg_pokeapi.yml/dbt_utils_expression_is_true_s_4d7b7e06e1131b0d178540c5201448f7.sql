



select
    1
from "nerd_facts"."public"."stg_pokeapi_pokemon"

where not(height > 0 OR IS NULL)

