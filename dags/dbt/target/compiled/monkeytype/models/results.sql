-- Main model in a dataset thats shows tests results

with base as (
    select *,
        -- Create bins for text size (used to determine best custom results)
        case when total_chars <= 500 then "small"
            when total_chars <= 1500 then "medium"
            when total_chars <= 4000 then "large"
            else "extra_large" end as text_size,
    from (
        select *, correctChars + incorrectChars + extraChars + missedChars as total_chars
        from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`results_incremental`
    ) as t1
),

best_custom_results as (
    select text_size, max(wpm) as best_custom_wpm,
    from base
    where mode = "custom"
    group by 1
)

select t1.*,
    -- Bool column to indicate best results so far
    case when t2.wpm is not null or t3.best_custom_wpm is not null then 1 else 0 end as is_best_result,
from base as t1

-- Join best results for each mode
left join `global-grammar-449122-b6`.`monkeytype_stats`.`best_results` as t2
    on t1.mode = t2.mode and t1.mode2 = CAST(t2.category AS STRING)
        and t1.punctuation = t2.punctuation and t1.numbers = t2.numbers
        and t1.language = t2.language and t1.wpm = t2.wpm

left join best_custom_results as t3
    on t1.text_size = t3.text_size and t1.wpm = t3.best_custom_wpm