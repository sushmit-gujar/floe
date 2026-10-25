select
    wiki,
    any_value(domain) as domain,
    count(*) as edits,
    round(count(*) filter (where is_bot) / count(*), 4) as bot_share,
    count(distinct user_name) as editors,
    count(*) filter (where change_type = 'new') as new_pages,
    sum(bytes_delta) as bytes_delta,
    max(event_time) as last_event_time
from {{ ref('wiki_edits') }}
group by wiki
order by edits desc
