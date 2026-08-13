from omigo_hydra import cluster_services_v2, cluster_common_v2
from omigo_hydra import cluster_protocol_v2
from omigo_hydra.cluster_services_v2 import SWFBuilder
from omigo_core import utils, dataframe
import sys, os, argparse


"""Example: School analytics using WfBlock pattern.
Demonstrates how WorkflowBlockDF-style reusable blocks simplify WF definitions
by encapsulating common multi-step operations into named methods.

Compare with university_100wf.py where patterns like "filter active students,
join with dept, group by department" are repeated inline across many WFs.
Here, SchoolBlockDF captures those patterns once and each WF becomes 1-2 lines.

Key concepts:
  1. SchoolBlockDF extends dataframe.DataFrame (same pattern as WorkflowBlockDF)
  2. Each method is a reusable pipeline: takes self (DataFrame), returns DataFrame
  3. Methods that join with other WFs take ctx to access ctx.read_df()
  4. Usage: df.extend_class(SchoolBlockDF).method_name(...)
  5. Blocks compose: filter_active().enrich_with_dept(ctx).summarize_by_dept()

DAG (6 WFs, 3 phases):
  Phase 0: students, enrollments, courses [roots - inline data]
  Phase 1: active_enrolled [join students + enrollments via block]
  Phase 2: dept_summary, course_summary [aggregations via block]

Run:
  python school_analytics_wfblock.py --mode local
  python school_analytics_wfblock.py --mode cluster
"""


# ============================================================
# WfBlock: Reusable workflow building blocks for school domain
# ============================================================
class SchoolBlockDF(dataframe.DataFrame):
    """Reusable school analytics blocks.

    Each method encapsulates a multi-step pattern that would otherwise be
    repeated inline across WF definitions. Compare with WorkflowBlockDF
    in omigo_crwd_wfs which does the same for security/telemetry operations.
    """
    def __init__(self, header_fields, data_fields):
        super().__init__(header_fields, data_fields)

    def filter_active_students(self, min_gpa = 2.0):
        """Filter to active students with GPA >= threshold.

        Expects columns: status, gpa.
        Produces: same columns, filtered rows.
        """
        return self \
            .eq_str("status", "active") \
            .ge_float("gpa", min_gpa)

    def enrich_with_dept(self, ctx):
        """Join with courses to add dept info, then join with departments for dept_name.

        Expects columns: dept_code (or course_id for course-based enrichment).
        Produces: adds dept_name column.
        """
        return self \
            .inner_map_join(ctx.read_df("departments"), ["dept_code"])

    def enrich_with_course(self, ctx):
        """Join with courses to add course_name and credits.

        Expects columns: course_id.
        Produces: adds course_name, dept_code, credits, level.
        """
        return self \
            .inner_map_join(ctx.read_df("courses"), ["course_id"])

    def summarize_by_dept(self):
        """Aggregate by department with count and sort.

        Expects columns: dept_code, dept_name.
        Produces columns: dept_code, dept_name, group:count.
        """
        return self \
            .group_count(["dept_code", "dept_name"]) \
            .sort("dept_code")


# ============================================================
# Main script
# ============================================================

# Command Line
parser = argparse.ArgumentParser(description = "SWF example: school analytics with WfBlock pattern")
parser.add_argument("--mode", choices = ["local", "cluster"], default = "local", help = "Execution mode: local (inmemory) or cluster")
args = parser.parse_args()

if (args.mode == "local"):
    os.environ.setdefault("HYDRA_PATH", "/tmp/hydra-v2-example")

# ============================================================
# 1. Initialize
# ============================================================
utils.info("=" * 60)
utils.info("1. Initializing (mode={})".format(args.mode))
utils.info("=" * 60)

if (args.mode == "cluster"):
    cluster_services_v2.init("school_analytics_wfblock")
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
    "semester": "Fall2026",
    "report_date": "2026-05-03",
}
utils.info("  params: {}".format(swf_params))

# ============================================================
# 5. Define WF blueprints using WfBlock
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("5. Defining WF blueprints (using SchoolBlockDF)")
utils.info("=" * 60)

# --- Phase 0: Root datasets ---

# WF: students (root, inline data)
# OUTPUT COLS: student_id, student_name, gpa, dept_code, status
students_bp = ctx.from_maps([
    {"student_id": "S001", "student_name": "Alice",   "gpa": "3.8", "dept_code": "CS",   "status": "active"},
    {"student_id": "S002", "student_name": "Bob",     "gpa": "2.9", "dept_code": "MATH", "status": "active"},
    {"student_id": "S003", "student_name": "Charlie", "gpa": "3.5", "dept_code": "CS",   "status": "inactive"},
    {"student_id": "S004", "student_name": "Diana",   "gpa": "3.2", "dept_code": "PHYS", "status": "active"},
    {"student_id": "S005", "student_name": "Eve",     "gpa": "3.9", "dept_code": "CS",   "status": "active"},
    {"student_id": "S006", "student_name": "Frank",   "gpa": "2.7", "dept_code": "MATH", "status": "active"},
    {"student_id": "S007", "student_name": "Grace",   "gpa": "3.6", "dept_code": "PHYS", "status": "active"},
    {"student_id": "S008", "student_name": "Hank",    "gpa": "3.1", "dept_code": "CS",   "status": "active"},
]).to_wf_spec()

utils.info("  students:        {} op(s)  [root, 8 rows]".format(len(students_bp.jobs_operations)))

# WF: enrollments (root, inline data)
# OUTPUT COLS: student_id, course_id, grade
enrollments_bp = ctx.from_maps([
    {"student_id": "S001", "course_id": "C301", "grade": "A"},
    {"student_id": "S001", "course_id": "C302", "grade": "A-"},
    {"student_id": "S004", "course_id": "C320", "grade": "B+"},
    {"student_id": "S005", "course_id": "C301", "grade": "A"},
    {"student_id": "S005", "course_id": "C302", "grade": "A"},
    {"student_id": "S007", "course_id": "C320", "grade": "A-"},
    {"student_id": "S007", "course_id": "C220", "grade": "A"},
    {"student_id": "S008", "course_id": "C201", "grade": "B"},
]).to_wf_spec()

utils.info("  enrollments:     {} op(s)  [root, 8 rows]".format(len(enrollments_bp.jobs_operations)))

# WF: courses (root, inline data)
# OUTPUT COLS: course_id, course_name, dept_code, credits
courses_bp = ctx.from_maps([
    {"course_id": "C201", "course_name": "Data Structures",    "dept_code": "CS",   "credits": "4"},
    {"course_id": "C301", "course_name": "Algorithms",         "dept_code": "CS",   "credits": "4"},
    {"course_id": "C302", "course_name": "Machine Learning",   "dept_code": "CS",   "credits": "4"},
    {"course_id": "C220", "course_name": "E&M Physics",        "dept_code": "PHYS", "credits": "4"},
    {"course_id": "C320", "course_name": "Quantum Mechanics",  "dept_code": "PHYS", "credits": "4"},
]).to_wf_spec()

utils.info("  courses:         {} op(s)  [root, 5 rows]".format(len(courses_bp.jobs_operations)))

# WF: departments (root, inline data)
# OUTPUT COLS: dept_code, dept_name, building
departments_bp = ctx.from_maps([
    {"dept_code": "CS",   "dept_name": "Computer Science", "building": "Turing Hall"},
    {"dept_code": "MATH", "dept_name": "Mathematics",      "building": "Euler Hall"},
    {"dept_code": "PHYS", "dept_name": "Physics",          "building": "Newton Hall"},
]).to_wf_spec()

utils.info("  departments:     {} op(s)  [root, 3 rows]".format(len(departments_bp.jobs_operations)))

# --- Phase 1: Joins using WfBlock ---
# Without WfBlock, this would be:
#   ctx.read_df("students").eq_str("status", "active").ge_float("gpa", 3.0)
#       .inner_map_join(ctx.read_df("enrollments"), ["student_id"])
#       .inner_map_join(ctx.read_df("courses"), ["course_id"])
#
# With WfBlock, the reusable parts become named methods:

# WF: active_enrolled — filter active students (block), join enrollments, enrich with course info (block)
# OUTPUT COLS: student_id, student_name, gpa, dept_code, status, course_id, grade, course_name, credits
active_enrolled_bp = ctx.read_df("students") \
    .extend_class(SchoolBlockDF) \
    .filter_active_students(bctx.min_gpa.value) \
    .inner_map_join(ctx.read_df("enrollments"), ["student_id"]) \
    .extend_class(SchoolBlockDF) \
    .enrich_with_course(ctx) \
    .to_wf_spec()

utils.info("  active_enrolled: {} op(s)  [filter_active + join enrollments + enrich_with_course]".format(
    len(active_enrolled_bp.jobs_operations)))

# --- Phase 2: Aggregations using WfBlock ---

# WF: dept_summary — enrich active_enrolled with dept_name (block), summarize by dept (block)
# OUTPUT COLS: dept_code, dept_name, group:count
dept_summary_bp = ctx.read_df("active_enrolled") \
    .select(["student_id", "dept_code"]) \
    .distinct() \
    .extend_class(SchoolBlockDF) \
    .enrich_with_dept(ctx) \
    .summarize_by_dept() \
    .to_wf_spec()

utils.info("  dept_summary:    {} op(s)  [enrich_with_dept + summarize_by_dept]".format(
    len(dept_summary_bp.jobs_operations)))

# WF: course_summary — group_count by course from active_enrolled
# OUTPUT COLS: course_id, course_name, group:count
course_summary_bp = ctx.read_df("active_enrolled") \
    .group_count(["course_id", "course_name"]) \
    .sort("course_name") \
    .to_wf_spec()

utils.info("  course_summary:  {} op(s)  [group_count by course]".format(
    len(course_summary_bp.jobs_operations)))

# ============================================================
# 6. Materialize SWF
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("6. Materializing SWF (mode={})".format(args.mode))
utils.info("=" * 60)

swf_id = (
    SWFBuilder(ctx, params = swf_params)
        .add_wf("students", students_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("enrollments", enrollments_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("courses", courses_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("departments", departments_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("active_enrolled", active_enrolled_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("dept_summary", dept_summary_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("course_summary", course_summary_bp,
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
    enrollments_out = ctx.read_output("enrollments")
    courses_out = ctx.read_output("courses")
    active_enrolled_out = ctx.read_output("active_enrolled")
    dept_summary_out = ctx.read_output("dept_summary")
    course_summary_out = ctx.read_output("course_summary")
else:
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
    enrollments_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "enrollments")
    courses_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "courses")
    active_enrolled_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "active_enrolled")
    dept_summary_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "dept_summary")
    course_summary_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "course_summary")

utils.info("")
utils.info("  students (root):       {} rows, cols: {}".format(students_out.num_rows(), students_out.get_columns()))
utils.info("  enrollments (root):    {} rows, cols: {}".format(enrollments_out.num_rows(), enrollments_out.get_columns()))
utils.info("  courses (root):        {} rows, cols: {}".format(courses_out.num_rows(), courses_out.get_columns()))
utils.info("  active_enrolled:       {} rows, cols: {}".format(active_enrolled_out.num_rows(), active_enrolled_out.get_columns()))
utils.info("  dept_summary:          {} rows, cols: {}".format(dept_summary_out.num_rows(), dept_summary_out.get_columns()))
utils.info("  course_summary:        {} rows, cols: {}".format(course_summary_out.num_rows(), course_summary_out.get_columns()))

# ============================================================
# Summary
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("DONE - School analytics WfBlock example completed")
utils.info("=" * 60)
utils.info("  Key takeaway: SchoolBlockDF methods replaced inline multi-step chains:")
utils.info("    filter_active_students() = eq_str('status','active') + ge_float('gpa', threshold)")
utils.info("    enrich_with_dept()       = inner_map_join(departments, ['dept_code'])")
utils.info("    enrich_with_course()     = inner_map_join(courses, ['course_id'])")
utils.info("    summarize_by_dept()      = group_count(['dept_code','dept_name']) + sort")
utils.info("")
utils.info("  In the 100wf example, these patterns repeat 10+ times inline.")
utils.info("  With WfBlock, define once, reuse everywhere.")
