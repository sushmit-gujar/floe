select
    wiki,
    event_minute,
    count(*) as edits,
    count(*) filter (where is_bot) as bot_edits,
    count(*) filter (where change_type = 'new') as new_pages,
    count(distinct user_name) as editors,
    sum(bytes_delta) as bytes_delta
from {{ ref('wiki_edits') }}
group by wiki, event_minute
