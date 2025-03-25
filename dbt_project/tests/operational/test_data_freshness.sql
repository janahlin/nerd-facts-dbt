-- This test checks if data has been updated within the last X days
-- For demonstration, we'll check if Star Wars data is fresh

with max_date_check as (
    select
        max(release_date) as latest_release_date
    from {{ ref('dim_films') }}
),

validation as (
    select
        latest_release_date,
        -- Replace with actual logic for your project
        -- This is just an example calculation
        (current_date - interval '10 years') as comparison_date
    from max_date_check
)

select *
from validation
where latest_release_date < comparison_date
-- If this query returns any rows, it means the data is not fresh enough
-- In a real scenario, you would adjust the interval based on your business requirements 