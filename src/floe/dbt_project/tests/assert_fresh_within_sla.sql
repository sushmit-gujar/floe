-- Contract: newest committed event must be younger than quality.freshness_sla_seconds.
select * from {{ ref('ingest_quality') }}
where freshness_seconds is null or freshness_seconds > {{ var('freshness_sla_seconds') }}
