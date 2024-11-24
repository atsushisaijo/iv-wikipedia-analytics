{{
    config(
        materialized='view'
    )
}}

-- select all from api-base table
select type, title, user, userid, timestamp, comment
from {{ source('wikipedia_api', 'wiki_edits') }}
where timestamp is not null
