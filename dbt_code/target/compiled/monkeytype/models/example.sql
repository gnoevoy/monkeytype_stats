with cte as (
    select extract(date from timestamp) as date, count(*) as tests_num
    from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`results`
    group by 1
)

select t1.*, COALESCE(t2.tests_num, 0) as tests_num
from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`activity` as t1
left join cte as t2     
    on t1.date = t2.date
where tests != tests_num