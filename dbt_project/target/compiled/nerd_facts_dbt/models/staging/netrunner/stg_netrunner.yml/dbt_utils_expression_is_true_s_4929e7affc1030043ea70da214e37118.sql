



select
    1
from "nerd_facts"."public"."stg_netrunner_cards"

where not(NOT (is_agenda = true AND side_code = 'runner'))

