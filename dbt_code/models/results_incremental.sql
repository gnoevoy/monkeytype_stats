-- Incremental model that appends new tests only
-- Will be used in "results" model for the following transformations

{{ config(materialized='incremental', unique_key="_id") }}

select *
from {{ source('monkeytype', 'results') }}

-- Append only new records based on timestamp
{% if is_incremental() %}
    where timestamp > (select max(timestamp) from {{ this }})
{% endif %}
