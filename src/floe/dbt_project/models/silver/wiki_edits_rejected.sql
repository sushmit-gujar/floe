select kafka_partition, kafka_offset, split_part(reason, ':', 1) as reason_class, reason,
       min(ingested_at) as ingested_at, count(*) as copies
from {{ source('bronze', 'wiki_edits_rejected') }}
group by all
