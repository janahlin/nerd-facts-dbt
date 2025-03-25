



select
    1
from "nerd_facts"."public"."stg_netrunner_packs"

where not(card_count > 0)

