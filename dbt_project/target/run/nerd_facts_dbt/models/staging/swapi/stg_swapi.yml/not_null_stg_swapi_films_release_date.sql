select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select release_date
from "nerd_facts"."public"."stg_swapi_films"
where release_date is null



      
    ) dbt_internal_test