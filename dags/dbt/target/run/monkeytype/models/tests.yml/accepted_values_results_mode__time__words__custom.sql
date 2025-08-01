
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        mode as value_field,
        count(*) as n_records

    from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`results`
    group by mode

)

select *
from all_values
where value_field not in (
    'time','words','custom'
)



  
  
      
    ) dbt_internal_test