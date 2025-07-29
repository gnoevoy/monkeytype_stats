
    
    

with dbt_test__target as (

  select _id as unique_field
  from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`results`
  where _id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


