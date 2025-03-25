



select
    1
from "nerd_facts"."public"."stg_netrunner_packs"

where not(release_date <= current_date OR IS NULL)

