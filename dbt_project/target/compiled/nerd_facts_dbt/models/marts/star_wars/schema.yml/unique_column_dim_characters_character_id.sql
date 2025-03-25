

with validation as (
    select
        character_id as column_value,
        count(*) as occurrences
    from "nerd_facts"."public"."dim_characters"
    where character_id is not null
    group by character_id
    having count(*) > 1
)

select *
from validation

