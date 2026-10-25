-- Platform guarantee: the stream processor commits each Kafka record exactly once,
-- i.e. no (partition, offset) appears twice in bronze. Upstream repeats of the same event
-- under different offsets are a source property, deduped in silver and reported as source_duplicates.
select * from {{ ref('ingest_quality') }} where bronze_duplicates > 0
