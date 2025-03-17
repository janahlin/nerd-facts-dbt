select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select max_atmosphering_speed
from "nerd_facts"."public"."stg_swapi_starships"
where max_atmosphering_speed is null



      
    ) dbt_internal_test