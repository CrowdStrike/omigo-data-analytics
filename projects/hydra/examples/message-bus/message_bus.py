from omigo_hydra import cluster_services_v2, cluster_common_v2, message_bus
from omigo_hydra.cluster_common_v2 import ClusterPaths
from omigo_hydra.message_bus import MessageBus, BucketRollup
from omigo_core import timefuncs
import sys, os, json, time

"""Example: Message Bus — publish events, read them back, run a 01min rollup.
Demonstrates:
  1. Connecting to a running cluster via cluster_services_v2.init()
  2. Publishing events to the message bus (incoming folder)
  3. Reading events back via greedy top-down read
  4. Running a 01min bucket rollup (incoming -> 01min bucket)
  5. Reading events again (now served from bucket file)
  6. Cleaning up processed incoming files

Run:
  python message_bus.py
"""

# ============================================================
# 1. Initialize cluster connection
# ============================================================
print("=" * 60)
print("1. Initializing cluster connection")
print("=" * 60)

cluster_services_v2.init("message_bus")
handler = cluster_services_v2.get_cluster_handler()
print(f"  HYDRA_PATH: {cluster_common_v2.HYDRA_PATH}")

# ============================================================
# 2. Ensure message bus directories exist
# ============================================================
print()
print("=" * 60)
print("2. Creating message bus directories")
print("=" * 60)

for path in [ClusterPaths.get_message_bus_base(),
             ClusterPaths.get_message_bus_incoming(),
             ClusterPaths.get_message_bus_completed(),
             ClusterPaths.get_message_bus_buckets()]:
    handler.create(path)
    print(f"  created: {path}")

for level in message_bus.MESSAGE_BUS_BUCKET_LEVELS:
    handler.create(ClusterPaths.get_message_bus_bucket(level))
print(f"  created bucket dirs for: {message_bus.MESSAGE_BUS_BUCKET_LEVELS}")

# ============================================================
# 3. Publish sample events
# ============================================================
print()
print("=" * 60)
print("3. Publishing sample events")
print("=" * 60)

mbus = MessageBus(handler)

# publish a few workflow events
msg1 = mbus.publish("platform", "swf", "swf-001", "checkpoint_completed",
    {"checkpoint_name": "filter_step", "num_rows": 42, "start_ts": 1000, "end_ts": 1060},
    dmsg = "example")

msg2 = mbus.publish("platform", "wf", "wf-007", "write_completed",
    {"operation_name": "output.write", "num_rows": 18, "start_ts": 1000, "end_ts": 1060},
    dmsg = "example")

msg3 = mbus.publish("platform", "swf", "swf-001", "checkpoint_completed",
    {"checkpoint_name": "enrich_step", "num_rows": 35, "start_ts": 1060, "end_ts": 1120},
    dmsg = "example")

print(f"  published: {msg1}")
print(f"  published: {msg2}")
print(f"  published: {msg3}")

# ============================================================
# 4. Read events from incoming
# ============================================================
print()
print("=" * 60)
print("4. Reading events (from incoming — no buckets yet)")
print("=" * 60)

now = timefuncs.get_utctimestamp_sec()
result = mbus.read("platform", now - 3600, now + 3600, dmsg = "example")
print(f"  rows read: {result.num_rows()}")
print(f"  columns:   {result.get_columns()}")

if (result.num_rows() > 0):
    message_ids = result.col_as_array("message_id")
    message_types = result.col_as_array("message_type")
    for i in range(result.num_rows()):
        print(f"    [{i}] {message_types[i]}: {message_ids[i]}")

# ============================================================
# 5. Run 01min rollup
# ============================================================
print()
print("=" * 60)
print("5. Running 01min rollup (incoming -> 01min bucket)")
print("=" * 60)

rollup = BucketRollup()

# compute the current 01min bucket boundary
interval_01min = message_bus.MESSAGE_BUS_BUCKET_INTERVALS_SECONDS[message_bus.BUCKET_01MIN]
from omigo_hydra import etl
bucket_start = etl.floor_to_bucket(now, interval_01min)
bucket_end = bucket_start + interval_01min

print(f"  bucket window: [{bucket_start}, {bucket_end})")

bucket_file = rollup.rollup_01min(handler, "platform", bucket_start, bucket_end, dmsg = "example")

if (bucket_file is not None):
    print(f"  bucket file created: {bucket_file}")
else:
    print(f"  no bucket file created (no messages in incoming)")

# ============================================================
# 6. Read events again (now from bucket)
# ============================================================
print()
print("=" * 60)
print("6. Reading events again (bucket + incoming)")
print("=" * 60)

result2 = mbus.read("platform", now - 3600, now + 3600, dmsg = "example")
print(f"  rows read: {result2.num_rows()}")

# filter by message_type
result3 = mbus.read("platform", now - 3600, now + 3600, message_type = "checkpoint_completed", dmsg = "example")
print(f"  checkpoint_completed only: {result3.num_rows()} rows")

# ============================================================
# 7. Cleanup processed incoming files
# ============================================================
print()
print("=" * 60)
print("7. Cleanup (delete completed incoming files)")
print("=" * 60)

# use threshold_seconds=0 to clean up immediately (for demo purposes)
rollup.cleanup_incoming(handler, "platform", threshold_seconds = 0, dmsg = "example")
print("  cleanup done")

# ============================================================
# Summary
# ============================================================
print()
print("=" * 60)
print("DONE — Message Bus example completed")
print("=" * 60)
print()
print("  Steps demonstrated:")
print("    1. Created message bus directory structure")
print("    2. Published 3 events (2 checkpoint_completed, 1 write_completed)")
print("    3. Read events from incoming (pre-rollup)")
print("    4. Rolled up incoming into 01min bucket file")
print("    5. Read events from bucket (post-rollup)")
print("    6. Filtered read by message_type")
print("    7. Cleaned up processed incoming files")
