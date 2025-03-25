
  
    

  create  table "nerd_facts"."public"."fct_multiple_sources_joined__dbt_tmp"
  
  
    as
  
  (
    -- this model finds cases where a model references more than one source
with direct_source_relationships as (
    select distinct
        child,
        parent
    from "nerd_facts"."public"."int_all_dag_relationships"
    where distance = 1
    and parent_resource_type = 'source'
    and not parent_is_excluded
    and not child_is_excluded
    -- we order the CTE so that listagg returns values correctly sorted for some warehouses
    order by 1, 2
),

multiple_sources_joined as (
    select
        child,
        
    string_agg(
        parent,
        ', '
        
        ) as source_parents
    from direct_source_relationships
    group by 1
    having count(*) > 1
)

select * from multiple_sources_joined



    

    
    

    

    


  );
  