

select *
from `global-grammar-449122-b6`.`monkeytype_stats`.`results`


    where timestamp > (select max(timestamp) from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`results_incremental`)
