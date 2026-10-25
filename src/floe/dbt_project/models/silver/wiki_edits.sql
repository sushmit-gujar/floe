-- Deduplicated, typed edits. Exactly-once ingest means this should drop nothing;
-- the dedupe is defence in depth for replays from other paths (see tests/).
-- Written as hash aggregate + hash join (both spill to disk in DuckDB) rather than a
-- full-width window function, so memory stays bounded at millions of rows.
with first_seen as (
    select
        event_id,
        arg_min(
            struct_pack(p := kafka_partition, o := kafka_offset),
            struct_pack(t := ingested_at, p := kafka_partition, o := kafka_offset)
        ) as k
    from {{ ref('stg_wiki_edits') }}
    group by event_id
)
select s.*
from {{ ref('stg_wiki_edits') }} as s
join first_seen as f
  on s.event_id = f.event_id
 and s.kafka_partition = f.k.p
 and s.kafka_offset = f.k.o
