
    
    

with all_values as (

    select
        type_name as value_field,
        count(*) as n_records

    from "nerd_facts"."public"."stg_pokeapi_types"
    group by type_name

)

select *
from all_values
where value_field not in (
    'normal','fire','water','electric','grass','ice','fighting','poison','ground','flying','psychic','bug','rock','ghost','dragon','dark','steel','fairy'
)


