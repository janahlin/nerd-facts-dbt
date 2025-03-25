
  create view "nerd_facts"."public"."fct_undocumented_sources__dbt_tmp"
    
    
  as (
    with

all_resources as (
    select * from "nerd_facts"."public"."int_all_graph_resources"
    where not is_excluded

),

final as (

    select distinct
        source_name

    from all_resources
    where not is_source_described and resource_type = 'source'

)

select * from final



    

    
    

    

    


  );