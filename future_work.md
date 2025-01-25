## Pipeline Latency Optimisation Under Flash Sale Load

### Measured Problem
End-to-end latency (Postgres commit → ClickHouse queryable) degrades 
significantly under flash sale load:
- Normal (10 TPS):    p50=982ms,   p95=1,409ms
- Flash sale (50 TPS): p50=6,249ms, p95=8,126ms
- Degradation factor: 6.4x slower under 5x load

### Root Causes Identified

1. Flink checkpoint interval (30s): Kafka sink commits offsets only
   after checkpoint. At 50 TPS, events wait up to 30s before being
   committed downstream. Fix: reduce CHECKPOINT_INTERVAL_MS from
   30000 to 5000.

2. ClickHouse Kafka Engine poll interval (default ~500ms): ClickHouse
   polls orders_materialized topic every 500ms. At 50 TPS, messages
   queue between polls. Fix: add kafka_max_block_size=1000 and
   kafka_poll_max_batch_size=1000 to Kafka Engine table settings.

3. Single Kafka partition: All CDC events flow through one partition,
   limiting parallelism. Fix: increase topic partitions to 3, increase
   Flink parallelism to match.

### Expected Improvement
Reducing checkpoint interval to 5s alone should bring flash sale p50
from ~6,249ms to ~2,000ms. Combined with ClickHouse poll tuning,
target is sub-2s p50 at 50 TPS.

### Why Not Fixed Now
Optimisation requires full stack rebuild and re-benchmarking.
Documented here for production hardening phase.
