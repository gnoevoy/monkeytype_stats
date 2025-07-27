select mode, category, count(*) as count
from `global-grammar-449122-b6`.`monkeytype_stats`.`best_results`
group by 1, 2