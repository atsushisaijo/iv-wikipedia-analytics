{{
    config(
        materialized='view'
    )
}}

-- Generate time series for the day -- 2024-10-31 is hardcoded -- can be moved to dbt-project.yaml
select
    sliding_window_start,
    --add 30 minutes of length
    sliding_window_start + interval '30 minutes' as sliding_window_end
from (
    -- generate starting time-series, sliding 15 minutes
    select
    CAST('{{ var("target_date") }}' AS TIMESTAMP)
        -- interval 'x minutes', handled similarly to timedelta in python
        + (INTERVAL '15 minutes' * timeseries) AS sliding_window_start

    FROM range(96) AS t(timeseries)
    -- Generate 96 values (0 to 95): 24 hours * 4 (15 minutes)
    -- t() is table alias in DuckDB
    )
