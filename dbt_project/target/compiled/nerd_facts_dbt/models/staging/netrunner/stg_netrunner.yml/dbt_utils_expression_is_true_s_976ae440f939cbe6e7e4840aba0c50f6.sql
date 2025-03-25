



select
    1
from "nerd_facts"."public"."stg_netrunner_cards"

where not(strength >= 0 OR IS NULL)

