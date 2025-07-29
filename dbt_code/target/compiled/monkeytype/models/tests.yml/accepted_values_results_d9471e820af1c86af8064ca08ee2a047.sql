
    
    

with all_values as (

    select
        language as value_field,
        count(*) as n_records

    from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`results`
    group by language

)

select *
from all_values
where value_field not in (
    'english','english_5k','english_1k'
)


