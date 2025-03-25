{% test not_null_column(model, column_name) %}

with validation as (
    select
        {{ column_name }} as column_value
    from {{ model }}
    where {{ column_name }} is null
)

select *
from validation
where column_value is null

{% endtest %} 