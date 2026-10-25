-- Events stamped more than 5 minutes after they were ingested indicate clock skew or bad data.
select * from {{ ref('wiki_edits') }} where event_time > ingested_at + interval 5 minute
