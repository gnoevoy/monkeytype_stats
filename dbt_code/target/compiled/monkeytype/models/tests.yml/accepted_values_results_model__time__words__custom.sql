
    
    

with all_values as (

    select
        model as value_field,
        count(*) as n_records

    from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`results`
    group by model

)

select *
from all_values
where value_field not in (
    'time','words','custom'
)


