from omigo_hydra import cluster_services_v2, cluster_common_v2
from omigo_hydra import cluster_protocol_v2
from omigo_hydra.cluster_services_v2 import SWFBuilder
from omigo_core import utils
import sys, os, argparse


"""Example: School analytics SWF with 4 WFs using the current API.
Demonstrates:
  1. ctx.from_maps() for root WFs with inline synthetic data
  2. ctx.read_df("upstream_name") for downstream WFs (proxy DataFrame, auto-resolved)
  3. SWFBuilder(ctx, params = {...}) with flat str->str params map
  4. Edge inference from ctx.read_df() calls (no manual input_ids/output_ids)
  5. inner_map_join with ctx.read_df() proxy as join operand
  6. Checkpoint (creates intermediate output_id automatically)
  7. BVar broadcast variables (bctx.min_gpa used as filter threshold)
  8. Variable bucket sizes (downstream 120s reading from upstream 60s)
  9. read_df("upstream_id", params = ["key"], prefix = "ctx") for param enrichment

DAG (inferred automatically from ctx.read_df calls):
  students (root, 60s buckets)     enrollments (root, 60s buckets)
      |                                |
      +-> enrollment_join (reads students + enrollments join, 120s buckets)
      |
      +-> summary (reads enrollment_join, aggregation, 120s buckets)

  Phase 0: students, enrollments [roots - inline data]
  Phase 1: enrollment_join       [join students with enrollments via ctx.read_df]
  Phase 2: summary               [aggregate by department from enrollment_join]

Note on seed_input vs from_maps for root WFs:
  - ctx.read_df("seed_input") is used when the root WF reads params/timestamps and
    fetches data from an external source (LogScale, Jira, etc.) using those params.
  - ctx.from_maps([...]) is used when the root WF starts with inline synthetic data.
    Edge inference still treats it as root and generates seed_input for params/timestamps.
  Both patterns are valid; this example uses from_maps() since we have no external sources.

Run:
  python school_analytics.py --mode local
  python school_analytics.py --mode cluster
"""

# Command Line
parser = argparse.ArgumentParser(description = "SWF example: school analytics (4 WFs)")
parser.add_argument("--mode", choices = ["local", "cluster"], default = "local", help = "Execution mode: local (inmemory) or cluster")
args = parser.parse_args()

# point at temp dir for local, real cluster for cluster mode
if (args.mode == "local"):
    os.environ.setdefault("HYDRA_PATH", "/tmp/hydra-v2-example")


# ============================================================
# 1. Initialize
# ============================================================
utils.info("=" * 60)
utils.info("1. Initializing (mode={})".format(args.mode))
utils.info("=" * 60)

utils.info("  HYDRA_PATH: {}".format(cluster_common_v2.HYDRA_PATH))

# ============================================================
# 2. Create ExecutorContext
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("2. Creating ExecutorContext")
utils.info("=" * 60)

if (args.mode == "local"):
    ctx = cluster_protocol_v2.ClusterExecutorContext(
        namespace = "platform",
        session_protocol = None,
    )
else:
    ctx = cluster_protocol_v2.ClusterExecutorContext(
        namespace = "platform",
        session_protocol = cluster_services_v2.SESSION_PROTOCOL,
    )

utils.info("  namespace: {}".format(ctx.namespace))
utils.info("  session_protocol: {}".format("None (local)" if (ctx.session_protocol is None) else "set"))

# ============================================================
# 3. Build broadcast variables
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("3. Building broadcast variables")
utils.info("=" * 60)

bctx = ctx.get_broadcast_context()
bctx.min_gpa = bctx.asFloat(3.0)

utils.info("  min_gpa = {}".format(bctx.min_gpa.value))

# ============================================================
# 4. Define SWF params
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("4. Defining SWF params")
utils.info("=" * 60)

swf_params = {
    "report_date": "2026-05-03",
    "min_gpa": "3.0",
}
utils.info("  params: {}".format(swf_params))

# ============================================================
# 5. Define WF blueprints
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("5. Defining WF blueprints")
utils.info("=" * 60)

# --- WF: students (root) ---
# Inline synthetic data via ctx.from_maps(). Filters active students with GPA >= min_gpa (BVar),
# checkpoints the filtered set as "student_stats", then sorts by GPA.
students_blueprint = ctx.from_maps([
    {"student_id": "S001", "name": "Alice",   "gpa": "3.8", "department": "CS",      "status": "active"},
    {"student_id": "S002", "name": "Bob",     "gpa": "2.9", "department": "Math",    "status": "active"},
    {"student_id": "S003", "name": "Charlie", "gpa": "3.5", "department": "CS",      "status": "inactive"},
    {"student_id": "S004", "name": "Diana",   "gpa": "3.2", "department": "Physics", "status": "active"},
    {"student_id": "S005", "name": "Eve",     "gpa": "3.9", "department": "CS",      "status": "active"},
    {"student_id": "S006", "name": "Frank",   "gpa": "2.7", "department": "Math",    "status": "active"},
    {"student_id": "S007", "name": "Grace",   "gpa": "3.6", "department": "Physics", "status": "active"},
    {"student_id": "S008", "name": "Hank",    "gpa": "3.1", "department": "CS",      "status": "active"},
]) \
    .eq_str("status", "active") \
    .ge_float("gpa", bctx.min_gpa.value) \
    .checkpoint("student_stats") \
    .sort("gpa") \
    .to_wf_spec()

utils.info("  students:        {} job op(s)  [root, inline data, filter, checkpoint, sort]".format(
    len(students_blueprint.jobs_operations)))

# --- WF: enrollments (root) ---
# Inline synthetic enrollment data as a separate root WF.
# Registered as a named WF so downstream WFs can reference it via ctx.read_df("enrollments").
enrollments_blueprint = ctx.from_maps([
    {"student_id": "S001", "course": "Algorithms",       "grade": "A",  "credits": "4"},
    {"student_id": "S001", "course": "Databases",         "grade": "A-", "credits": "3"},
    {"student_id": "S004", "course": "Quantum Mechanics", "grade": "B+", "credits": "4"},
    {"student_id": "S005", "course": "Machine Learning",  "grade": "A",  "credits": "4"},
    {"student_id": "S005", "course": "Compilers",         "grade": "A",  "credits": "3"},
    {"student_id": "S007", "course": "Thermodynamics",    "grade": "A-", "credits": "4"},
    {"student_id": "S007", "course": "Optics",            "grade": "B+", "credits": "3"},
    {"student_id": "S008", "course": "Operating Systems", "grade": "B",  "credits": "4"},
]).to_wf_spec()

utils.info("  enrollments:     {} job op(s)  [root, inline data]".format(
    len(enrollments_blueprint.jobs_operations)))

# --- WF: enrollment_join (downstream of students + enrollments) ---
# Reads upstream "students" output via ctx.read_df proxy. Joins with upstream "enrollments"
# via ctx.read_df proxy on student_id using inner_map_join. Demonstrates param enrichment:
# report_date column is added from the WF's seed_input params via params=["report_date"], prefix="ctx".
enrollment_join_blueprint = ctx.read_df("students", params = ["report_date"], prefix = "ctx") \
    .inner_map_join(ctx.read_df("enrollments"), ["student_id"]) \
    .to_wf_spec()

utils.info("  enrollment_join: {} job op(s)  [reads students + enrollments, join on student_id, param enrichment]".format(
    len(enrollment_join_blueprint.jobs_operations)))

# --- WF: summary (downstream of enrollment_join) ---
# Reads enrollment_join output, aggregates by department using group_count.
# Demonstrates chained dependency (students -> enrollment_join -> summary)
# and variable bucket size (120s reading from upstream that also runs at 120s).
summary_blueprint = ctx.read_df("enrollment_join") \
    .select(["department", "course"]) \
    .group_count(["department"]) \
    .sort("department") \
    .to_wf_spec()

utils.info("  summary:         {} job op(s)  [reads enrollment_join, group_count by department]".format(
    len(summary_blueprint.jobs_operations)))

# ============================================================
# 6. Materialize SWF
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("6. Materializing SWF (mode={})".format(args.mode))
utils.info("=" * 60)

swf_id = (
    SWFBuilder(ctx, params = swf_params)
        .add_wf("students", students_blueprint,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("enrollments", enrollments_blueprint,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("enrollment_join", enrollment_join_blueprint,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("summary", summary_blueprint,
                 is_live = True, bucket_interval = 10, duration = 30)
        .materialize()
)

utils.info("  SWF result: {}".format(swf_id))

# ============================================================
# 7. Read and display results
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("7. Results")
utils.info("=" * 60)

if (args.mode == "local"):
    students_out = ctx.read_output("students")
    student_stats_out = ctx.read_output("student_stats")
    enrollments_out = ctx.read_output("enrollments")
    enrollment_join_out = ctx.read_output("enrollment_join")
    summary_out = ctx.read_output("summary")
else:
    # wait for SWF completion
    final_state = cluster_services_v2.wait_for_swf_completion("platform", swf_id)
    status = cluster_services_v2.get_swf_status("platform", swf_id)
    utils.info("  SWF final state: {}".format(status["swf_state"]))
    for wf_info in status["wfs"]:
        label = wf_info["name"] if (wf_info["name"] != "") else wf_info["wf_id"]
        utils.info("    WF {} ({}): {}".format(label, wf_info["wf_id"], wf_info["state"]))

    if (not status["is_successful"]):
        utils.info("  ERROR: SWF failed")
        sys.exit(1)

    students_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "students")
    student_stats_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "student_stats")
    enrollments_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "enrollments")
    enrollment_join_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "enrollment_join")
    summary_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "summary")

utils.info("")
utils.info("  students (sorted by GPA):    {} rows, cols: {}".format(students_out.num_rows(), students_out.get_columns()))
utils.info("  student_stats (checkpoint):  {} rows".format(student_stats_out.num_rows()))
utils.info("  enrollments (root):          {} rows, cols: {}".format(enrollments_out.num_rows(), enrollments_out.get_columns()))
utils.info("  enrollment_join:             {} rows, cols: {}".format(enrollment_join_out.num_rows(), enrollment_join_out.get_columns()))
utils.info("  summary (by department):     {} rows, cols: {}".format(summary_out.num_rows(), summary_out.get_columns()))

# ============================================================
# Summary
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("DONE - School analytics SWF completed")
utils.info("=" * 60)
utils.info("  students        -> {} rows  [root, filter gpa >= {}, checkpoint, sort]".format(
    students_out.num_rows(), bctx.min_gpa.value))
utils.info("  enrollments     -> {} rows  [root, inline enrollment data]".format(
    enrollments_out.num_rows()))
utils.info("  enrollment_join -> {} rows  [join students + enrollments, param enrichment: report_date]".format(
    enrollment_join_out.num_rows()))
utils.info("  summary         -> {} rows  [group_count by department from enrollment_join]".format(
    summary_out.num_rows()))
