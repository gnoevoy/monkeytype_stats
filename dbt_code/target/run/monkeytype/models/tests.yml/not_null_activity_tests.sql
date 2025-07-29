
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tests
from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`activity`
where tests is null



  
  
      
    ) dbt_internal_test