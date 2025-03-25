-- This test ensures that all Star Wars characters have the required attributes
-- and that the data meets our quality standards

with character_validation as (
    select
        name,
        height,
        mass,
        gender,
        case
            when name is null then 'Missing name'
            when height is null then 'Missing height'
            when trim(name) = '' then 'Empty name'
            when mass <= 0 and mass is not null then 'Invalid mass'
            when height <= 0 and height is not null then 'Invalid height'
            else 'Valid'
        end as validation_status
    from {{ ref('dim_characters') }}
)

select *
from character_validation
where validation_status != 'Valid'
-- If this query returns any rows, it means some characters have quality issues
-- that need to be addressed 