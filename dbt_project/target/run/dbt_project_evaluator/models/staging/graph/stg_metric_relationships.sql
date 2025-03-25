
  create view "nerd_facts"."public"."stg_metric_relationships__dbt_tmp"
    
    
  as (
    with 

_base_metric_relationships as (
    select * from "nerd_facts"."public"."base_metric_relationships"
),

final as (
    select 
        md5(cast(coalesce(cast(resource_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(direct_parent_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as unique_id, 
        *
    from _base_metric_relationships
)

select distinct * from final
  );