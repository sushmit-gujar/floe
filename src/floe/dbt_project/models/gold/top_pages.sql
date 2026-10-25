select wiki, title, edits, editors, rank
from (
    select wiki, title, count(*) as edits, count(distinct user_name) as editors,
           row_number() over (order by count(*) desc, wiki, title) as rank
    from {{ ref('wiki_edits') }}
    where change_type in ('edit', 'new')
    group by wiki, title
)
where rank <= 100
