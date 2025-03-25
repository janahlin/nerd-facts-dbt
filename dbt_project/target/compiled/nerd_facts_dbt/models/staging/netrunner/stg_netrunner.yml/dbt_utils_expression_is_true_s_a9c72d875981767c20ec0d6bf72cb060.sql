



select
    1
from "nerd_facts"."public"."stg_netrunner_cards"

where not((advancement_cost IS NULL AND agenda_points IS NULL) OR (advancement_cost IS NOT NULL AND agenda_points IS NOT NULL))

