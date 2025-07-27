select EXTRACT(MONTH FROM date) AS month, max(tests) as max_tests
from {{ source('monkeytype', 'activity') }}
group by 1 