
  create view "nerd_facts"."public"."fct_sources_without_freshness__dbt_tmp"
    
    
  as (
    with

all_resources as (
    select * from "nerd_facts"."public"."int_all_graph_resources"
    where not is_excluded

),

final as (

    select distinct
        resource_name

    from all_resources
    where not is_freshness_enabled and resource_type = 'source'

)

select * from final



    

    
    

    

    


  );