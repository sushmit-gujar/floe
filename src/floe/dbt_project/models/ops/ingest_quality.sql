with a as (
    select count(*) as accepted,
           count(*) filter (where is_late) as late,
           max(event_time) as max_event_time,
           max(ingested_at) as max_ingested_at
    from {{ ref('wiki_edits') }}
),
-- bronze_duplicates: the processor guarantee (one row per Kafka record).
-- source_duplicates: the same event delivered twice upstream (deduped in silver, reported here).
b as (
    select count(*) as bronze_rows,
           count(distinct (kafka_partition, kafka_offset)) as bronze_distinct_records,
           count(distinct event_id) as bronze_distinct_events
    from {{ ref('stg_wiki_edits') }}
),
r as (select count(*) as rejected from {{ ref('wiki_edits_rejected') }})
select
    current_timestamp as measured_at,
    a.accepted,
    r.rejected,
    case when a.accepted + r.rejected = 0 then 0
         else round(r.rejected / (a.accepted + r.rejected), 5) end as reject_rate,
    a.late,
    b.bronze_rows,
    b.bronze_rows - b.bronze_distinct_records as bronze_duplicates,
    b.bronze_rows - b.bronze_distinct_events as source_duplicates,
    a.max_event_time,
    a.max_ingested_at,
    epoch(current_timestamp) - epoch(a.max_ingested_at) as freshness_seconds
from a, b, r
