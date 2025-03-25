

with validation as (
    select
        character_id as column_value
    from "nerd_facts"."public"."dim_characters"
    where character_id is null
)

select *
from validation
where column_value is null

