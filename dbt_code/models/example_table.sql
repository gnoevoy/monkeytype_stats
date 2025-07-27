select mode, category, count(*) as count
from {{ source('monkeytype', 'best_results') }}
group by 1, 2