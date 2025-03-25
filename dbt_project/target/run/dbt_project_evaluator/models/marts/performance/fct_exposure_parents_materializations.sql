
  create view "nerd_facts"."public"."fct_exposure_parents_materializations__dbt_tmp"
    
    
  as (
    with 

direct_exposure_relationships as (
    select * from "nerd_facts"."public"."int_all_dag_relationships"
    where 
        distance = 1
        and child_resource_type = 'exposure'
        and ((
                parent_resource_type = 'model'
                and parent_materialized in ('view', 'ephemeral')
            )
            or (
                parent_resource_type = 'source'
            )
        )
        -- no test on child_is_excluded because exposures are never excluded
        and not parent_is_excluded
),

final as (

    select 
        parent_resource_type,
        parent as parent_resource_name,
        child as exposure_name,
        parent_materialized as parent_model_materialization

    from direct_exposure_relationships

)

select * from final



    

    
    

    

    


  );