



select
    1
from "nerd_facts"."public"."stg_netrunner_cards"

where not(is_ice is_ice = false OR strength IS NOT NULL)

