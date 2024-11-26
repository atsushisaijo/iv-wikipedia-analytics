--materialized to be validated
{{
    config(
        materialized='table'
    )
}}

select
ts.sliding_window_start,
ts.sliding_window_end,
count(*) as changes_count,
count(distinct we.user) as unique_users
from {{ ref('timeseries_base') }} ts  --timeseries
left join {{ ref('api_edits_base') }} we  --wiki edit
    on we.timestamp >= ts.sliding_window_start
    and we.timestamp < ts.sliding_window_end
group by 1, 2
order by changes_count desc
