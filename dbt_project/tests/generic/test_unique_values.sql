{% test unique_column(model, column_name) %}

with validation as (
    select
        {{ column_name }} as column_value,
        count(*) as occurrences
    from {{ model }}
    where {{ column_name }} is not null
    group by {{ column_name }}
    having count(*) > 1
)

select *
from validation

{% endtest %} 