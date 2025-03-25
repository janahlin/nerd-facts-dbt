



select
    1
from "nerd_facts"."public"."stg_netrunner_cards"

where not(advancement_cost >= 0 OR IS NULL)

