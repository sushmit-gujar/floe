select wiki, event_minute, count(*) from {{ ref('edits_per_minute') }} group by 1, 2 having count(*) > 1
