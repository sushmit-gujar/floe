-- Contract: share of events rejected at ingest must stay under quality.max_reject_rate.
select * from {{ ref('ingest_quality') }} where reject_rate > {{ var('max_reject_rate') }}
