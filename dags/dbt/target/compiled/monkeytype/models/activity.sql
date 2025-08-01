-- Table that tracks number of tests for each day
-- Added streaks count to track consecutive days of activity

with source as (
    select 
        date,
        -- Extracting date components
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(QUARTER FROM date) AS quarter,
        EXTRACT(DAY FROM date) AS day,
        EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
        FORMAT_DATE('%A', date) AS day_name,
        tests,
        -- Get previous day tests
        LAG(tests) OVER (ORDER BY date) AS previous_tests,
    from `global-grammar-449122-b6`.`monkeytype_stats`.`activity`
), 

streaks as (
    select *,
        -- Determine if the current day starts a new streak
        case when (previous_tests is null or previous_tests = 0) and (tests != 0 and tests is not null) then 1 else 0 end as streak_start,
    from source
),

streaks_with_id as (
    select *, 
        -- Cumulative sum to assign a unique ID to each streak
        sum(streak_start) over (order by date) as streak_id,
    from streaks
),

streaks_with_length as (
    select *,
        -- Define the streak length for each day
        case when tests = 0 then 0 else row_number() over (partition by streak_id order by date) end as current_streak,
    from streaks_with_id
)

select
    date,
    year,
    month,
    quarter,
    day,
    day_of_week,
    day_name,
    tests,
    current_streak,
from streaks_with_length
order by date desc