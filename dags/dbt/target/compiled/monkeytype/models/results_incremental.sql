-- Incremental model that appends new tests only
-- Will be used in "results" model for the following transformations



select *
from `global-grammar-449122-b6`.`monkeytype_stats`.`results`

-- Append only new records based on timestamp

    where timestamp > (select max(timestamp) from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`results_incremental`)
