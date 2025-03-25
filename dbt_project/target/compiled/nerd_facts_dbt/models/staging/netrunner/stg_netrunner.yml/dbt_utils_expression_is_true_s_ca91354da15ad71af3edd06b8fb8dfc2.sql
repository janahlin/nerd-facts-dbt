



select
    1
from "nerd_facts"."public"."stg_netrunner_cards"

where not(is_agenda is_agenda = false OR agenda_points IS NOT NULL)

