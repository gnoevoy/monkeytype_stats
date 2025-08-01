-- Table that provides general stats (fresh data from api)
-- All columns except "started_tests" could be calculated with the help of "results" table

select *
from {{ source('monkeytype', 'stats') }}