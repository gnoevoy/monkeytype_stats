
  
    

    create or replace table `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`second_example`
      
    
    

    
    OPTIONS()
    as (
      select EXTRACT(MONTH FROM date) AS month, max(tests) as max_tests
from `global-grammar-449122-b6`.`monkeytype_stats`.`activity`
group by 1
    );
  