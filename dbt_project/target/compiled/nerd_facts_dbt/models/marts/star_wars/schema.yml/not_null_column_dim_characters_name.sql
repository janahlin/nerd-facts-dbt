

with validation as (
    select
        name as column_value
    from "nerd_facts"."public"."dim_characters"
    where name is null
)

select *
from validation
where column_value is null

