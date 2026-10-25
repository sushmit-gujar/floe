-- Gold aggregates must account for every silver row.
select g.total, s.total
from (select sum(edits) as total from {{ ref('edits_per_minute') }}) g,
     (select count(*) as total from {{ ref('wiki_edits') }}) s
where g.total != s.total
