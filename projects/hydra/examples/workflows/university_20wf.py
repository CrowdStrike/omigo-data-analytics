from omigo_hydra import cluster_services_v2, cluster_common_v2
from omigo_hydra import cluster_protocol_v2
from omigo_hydra.cluster_services_v2 import SWFBuilder
from omigo_core import utils
import sys, os, argparse


"""Example: University analytics SWF with 20 WFs — comprehensive DAG patterns.

Demonstrates:
  1. Multiple root WFs (5 independent data sources)
  2. Fan-out (students feeds active_students -> student_grades + student_dept)
  3. Fan-in (full_transcript reads student_grades + courses)
  4. Diamond pattern (active_students -> ... -> honor_roll <- active_students)
  5. Multi-phase processing (5 phases, 0-4)
  6. Variable bucket sizes (60s -> 120s -> 240s)
  7. ctx.from_maps() for root WFs with inline synthetic data
  8. ctx.read_df("upstream") for downstream WFs (proxy DataFrame, auto-resolved)
  9. inner_map_join with rsuffix for column conflict resolution
  10. Checkpoint (intermediate output_id)
  11. BVar broadcast variables (bctx.min_gpa)
  12. SWF params (semester, report_date)
  13. read_df("upstream", params = ["key"], prefix = "ctx") for param enrichment
  14. group_count aggregation
  15. select (column pruning)
  16. drop_cols, rename, sort, add_seq_num for final reports
  17. Edge inference — no manual input_ids/output_ids anywhere

DAG (20 WFs, 5 phases):

  Phase 0 — Roots (5 WFs, 60s):
    students      courses      enrollments      faculty      departments

  Phase 1 — Filters & transforms (5 WFs, 60s):
    active_students <- students        [eq_str status=active, ge_float gpa>=min_gpa, checkpoint, sort]
    upper_courses   <- courses         [ge_float level>=300, checkpoint]
    passing_grades  <- enrollments     [not_eq_str grade!=F]
    tenured_faculty <- faculty         [not_eq_str title!=Adjunct]
    dept_lookup     <- departments     [select dept_code+dept_name only]

  Phase 2 — Enrichment joins (5 WFs, 120s):
    student_grades     <- active_students x passing_grades   [on student_id]
    course_enrollment  <- passing_grades x courses           [on course_id]
    faculty_dept       <- tenured_faculty x dept_lookup      [on dept_code]
    student_dept       <- active_students x dept_lookup      [on dept_code, param enrichment]
    full_transcript    <- student_grades x courses           [on course_id, rsuffix=course_info]

  Phase 3 — Aggregations (3 WFs, 120s):
    student_load     <- full_transcript    [group_count by student_id+student_name]
    course_demand    <- course_enrollment  [group_count by course_id+course_name]
    dept_enrollment  <- student_dept       [group_count by dept_code+dept_name]

  Phase 4 — Final reports (2 WFs, 240s):
    honor_roll   <- student_load x active_students  [on student_id, rsuffix=student_info]
    dept_report  <- dept_enrollment x faculty_dept   [on dept_code, rsuffix=faculty_info]

Run:
  python university_20wf.py --mode local
  python university_20wf.py --mode cluster
"""

# Command Line
parser = argparse.ArgumentParser(description = "SWF example: university analytics (20 WFs, 5 phases)")
parser.add_argument("--mode", choices = ["local", "cluster"], default = "local", help = "Execution mode: local (inmemory) or cluster")
args = parser.parse_args()

# point at temp dir for local, real cluster for cluster mode
if (args.mode == "local"):
    os.environ.setdefault("HYDRA_PATH", "/tmp/hydra-v2-example")

# ============================================================
# 1. Initialize
# ============================================================
utils.info("=" * 70)
utils.info("1. Initializing (mode={})".format(args.mode))
utils.info("=" * 70)

if (args.mode == "cluster"):
    cluster_services_v2.init("university_20wf")
utils.info("  HYDRA_PATH: {}".format(cluster_common_v2.HYDRA_PATH))

# ============================================================
# 2. Create ExecutorContext
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("2. Creating ExecutorContext")
utils.info("=" * 70)

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
utils.info("=" * 70)
utils.info("3. Building broadcast variables")
utils.info("=" * 70)

bctx = ctx.get_broadcast_context()
bctx.min_gpa = bctx.asFloat(2.0)

utils.info("  min_gpa = {}".format(bctx.min_gpa.value))

# ============================================================
# 4. Define SWF params
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("4. Defining SWF params")
utils.info("=" * 70)

swf_params = {
    "semester": "Fall2026",
    "report_date": "2026-05-03",
}
utils.info("  params: {}".format(swf_params))

# ============================================================
# 5. Define WF blueprints (20 WFs in 5 phases)
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("5. Defining WF blueprints (20 WFs)")
utils.info("=" * 70)

# ------------------------------------------------------------------
# Phase 0 — Root WFs (5 WFs): inline synthetic data via ctx.from_maps()
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 0: Root datasets ---")

# --- WF: students (root, 12 rows) ---
students_bp = ctx.from_maps([
    {"student_id": "S001", "student_name": "Alice",    "gpa": "3.8", "dept_code": "CS",   "year": "4", "status": "active"},
    {"student_id": "S002", "student_name": "Bob",      "gpa": "1.9", "dept_code": "MATH", "year": "3", "status": "active"},
    {"student_id": "S003", "student_name": "Charlie",  "gpa": "3.5", "dept_code": "CS",   "year": "2", "status": "inactive"},
    {"student_id": "S004", "student_name": "Diana",    "gpa": "3.2", "dept_code": "PHYS", "year": "4", "status": "active"},
    {"student_id": "S005", "student_name": "Eve",      "gpa": "3.9", "dept_code": "CS",   "year": "3", "status": "active"},
    {"student_id": "S006", "student_name": "Frank",    "gpa": "2.7", "dept_code": "MATH", "year": "2", "status": "active"},
    {"student_id": "S007", "student_name": "Grace",    "gpa": "3.6", "dept_code": "PHYS", "year": "4", "status": "active"},
    {"student_id": "S008", "student_name": "Hank",     "gpa": "2.1", "dept_code": "CHEM", "year": "1", "status": "active"},
    {"student_id": "S009", "student_name": "Ivy",      "gpa": "3.3", "dept_code": "BIO",  "year": "3", "status": "active"},
    {"student_id": "S010", "student_name": "Jake",     "gpa": "2.5", "dept_code": "CHEM", "year": "2", "status": "inactive"},
    {"student_id": "S011", "student_name": "Karen",    "gpa": "3.7", "dept_code": "BIO",  "year": "4", "status": "active"},
    {"student_id": "S012", "student_name": "Leo",      "gpa": "1.8", "dept_code": "MATH", "year": "1", "status": "active"},
]).to_wf_spec()

utils.info("  students:          {} op(s)  [root, 12 rows]".format(len(students_bp.jobs_operations)))

# --- WF: courses (root, 10 rows) ---
courses_bp = ctx.from_maps([
    {"course_id": "C101", "course_name": "Intro to CS",       "dept_code": "CS",   "credits": "3", "level": "100"},
    {"course_id": "C201", "course_name": "Data Structures",   "dept_code": "CS",   "credits": "4", "level": "200"},
    {"course_id": "C301", "course_name": "Algorithms",        "dept_code": "CS",   "credits": "4", "level": "300"},
    {"course_id": "C302", "course_name": "Machine Learning",  "dept_code": "CS",   "credits": "4", "level": "300"},
    {"course_id": "C110", "course_name": "Calculus I",        "dept_code": "MATH", "credits": "4", "level": "100"},
    {"course_id": "C210", "course_name": "Linear Algebra",    "dept_code": "MATH", "credits": "3", "level": "200"},
    {"course_id": "C120", "course_name": "General Physics",   "dept_code": "PHYS", "credits": "4", "level": "100"},
    {"course_id": "C320", "course_name": "Quantum Mechanics", "dept_code": "PHYS", "credits": "4", "level": "300"},
    {"course_id": "C130", "course_name": "Gen Chemistry",     "dept_code": "CHEM", "credits": "4", "level": "100"},
    {"course_id": "C140", "course_name": "Intro Biology",     "dept_code": "BIO",  "credits": "3", "level": "100"},
]).to_wf_spec()

utils.info("  courses:           {} op(s)  [root, 10 rows]".format(len(courses_bp.jobs_operations)))

# --- WF: enrollments (root, 20 rows) ---
enrollments_bp = ctx.from_maps([
    {"student_id": "S001", "course_id": "C301", "grade": "A",  "semester": "Fall2026"},
    {"student_id": "S001", "course_id": "C302", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S002", "course_id": "C110", "grade": "C",  "semester": "Fall2026"},
    {"student_id": "S002", "course_id": "C210", "grade": "F",  "semester": "Fall2026"},
    {"student_id": "S004", "course_id": "C320", "grade": "B+", "semester": "Fall2026"},
    {"student_id": "S004", "course_id": "C120", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S005", "course_id": "C301", "grade": "A",  "semester": "Fall2026"},
    {"student_id": "S005", "course_id": "C302", "grade": "A",  "semester": "Fall2026"},
    {"student_id": "S005", "course_id": "C201", "grade": "A",  "semester": "Fall2026"},
    {"student_id": "S006", "course_id": "C110", "grade": "B",  "semester": "Fall2026"},
    {"student_id": "S006", "course_id": "C210", "grade": "B-", "semester": "Fall2026"},
    {"student_id": "S007", "course_id": "C320", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S007", "course_id": "C120", "grade": "A",  "semester": "Fall2026"},
    {"student_id": "S008", "course_id": "C130", "grade": "C+", "semester": "Fall2026"},
    {"student_id": "S009", "course_id": "C140", "grade": "A",  "semester": "Fall2026"},
    {"student_id": "S009", "course_id": "C301", "grade": "B+", "semester": "Fall2026"},
    {"student_id": "S011", "course_id": "C140", "grade": "A",  "semester": "Fall2026"},
    {"student_id": "S011", "course_id": "C301", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S012", "course_id": "C110", "grade": "F",  "semester": "Fall2026"},
    {"student_id": "S003", "course_id": "C201", "grade": "B",  "semester": "Fall2026"},
]).to_wf_spec()

utils.info("  enrollments:       {} op(s)  [root, 20 rows]".format(len(enrollments_bp.jobs_operations)))

# --- WF: faculty (root, 8 rows) ---
faculty_bp = ctx.from_maps([
    {"faculty_id": "F01", "faculty_name": "Dr. Smith",    "dept_code": "CS",   "title": "Professor"},
    {"faculty_id": "F02", "faculty_name": "Dr. Johnson",  "dept_code": "CS",   "title": "Associate"},
    {"faculty_id": "F03", "faculty_name": "Dr. Williams", "dept_code": "MATH", "title": "Professor"},
    {"faculty_id": "F04", "faculty_name": "Dr. Brown",    "dept_code": "PHYS", "title": "Adjunct"},
    {"faculty_id": "F05", "faculty_name": "Dr. Davis",    "dept_code": "PHYS", "title": "Professor"},
    {"faculty_id": "F06", "faculty_name": "Dr. Miller",   "dept_code": "CHEM", "title": "Associate"},
    {"faculty_id": "F07", "faculty_name": "Dr. Wilson",   "dept_code": "BIO",  "title": "Professor"},
    {"faculty_id": "F08", "faculty_name": "Dr. Moore",    "dept_code": "MATH", "title": "Adjunct"},
]).to_wf_spec()

utils.info("  faculty:           {} op(s)  [root, 8 rows]".format(len(faculty_bp.jobs_operations)))

# --- WF: departments (root, 5 rows) ---
departments_bp = ctx.from_maps([
    {"dept_code": "CS",   "dept_name": "Computer Science", "building": "Turing Hall",   "budget": "5000000"},
    {"dept_code": "MATH", "dept_name": "Mathematics",      "building": "Euler Hall",    "budget": "3000000"},
    {"dept_code": "PHYS", "dept_name": "Physics",          "building": "Newton Hall",   "budget": "4000000"},
    {"dept_code": "CHEM", "dept_name": "Chemistry",        "building": "Curie Hall",    "budget": "3500000"},
    {"dept_code": "BIO",  "dept_name": "Biology",          "building": "Darwin Hall",   "budget": "3200000"},
]).to_wf_spec()

utils.info("  departments:       {} op(s)  [root, 5 rows]".format(len(departments_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 1 — Filters & transforms (5 WFs)
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 1: Filters & transforms ---")

# --- WF: active_students <- students ---
# Filter active students with GPA >= min_gpa (BVar), checkpoint, sort by gpa descending
active_students_bp = ctx.read_df("students") \
    .eq_str("status", "active") \
    .ge_float("gpa", bctx.min_gpa.value) \
    .checkpoint("active_ckpt") \
    .sort("gpa") \
    .to_wf_spec()

utils.info("  active_students:   {} op(s)  [eq_str status=active, ge_float gpa>={}, checkpoint, sort]".format(
    len(active_students_bp.jobs_operations), bctx.min_gpa.value))

# --- WF: upper_courses <- courses ---
# Filter courses with level >= 300, checkpoint
upper_courses_bp = ctx.read_df("courses") \
    .ge_float("level", 300.0) \
    .checkpoint("upper_ckpt") \
    .to_wf_spec()

utils.info("  upper_courses:     {} op(s)  [ge_float level>=300, checkpoint]".format(
    len(upper_courses_bp.jobs_operations)))

# --- WF: passing_grades <- enrollments ---
# Exclude failing grades (grade != F)
passing_grades_bp = ctx.read_df("enrollments") \
    .not_eq_str("grade", "F") \
    .to_wf_spec()

utils.info("  passing_grades:    {} op(s)  [not_eq_str grade!=F]".format(
    len(passing_grades_bp.jobs_operations)))

# --- WF: tenured_faculty <- faculty ---
# Exclude adjunct faculty (title != Adjunct)
tenured_faculty_bp = ctx.read_df("faculty") \
    .not_eq_str("title", "Adjunct") \
    .to_wf_spec()

utils.info("  tenured_faculty:   {} op(s)  [not_eq_str title!=Adjunct]".format(
    len(tenured_faculty_bp.jobs_operations)))

# --- WF: dept_lookup <- departments ---
# Slim join table: only dept_code + dept_name (drop building, budget)
dept_lookup_bp = ctx.read_df("departments") \
    .select(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  dept_lookup:       {} op(s)  [select dept_code+dept_name]".format(
    len(dept_lookup_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 2 — Enrichment joins (5 WFs)
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 2: Enrichment joins ---")

# --- WF: student_grades <- active_students x passing_grades [on student_id] ---
student_grades_bp = ctx.read_df("active_students") \
    .inner_map_join(ctx.read_df("passing_grades"), ["student_id"]) \
    .to_wf_spec()

utils.info("  student_grades:    {} op(s)  [active_students x passing_grades on student_id]".format(
    len(student_grades_bp.jobs_operations)))

# --- WF: course_enrollment <- passing_grades x courses [on course_id] ---
course_enrollment_bp = ctx.read_df("passing_grades") \
    .inner_map_join(ctx.read_df("courses"), ["course_id"]) \
    .to_wf_spec()

utils.info("  course_enrollment: {} op(s)  [passing_grades x courses on course_id]".format(
    len(course_enrollment_bp.jobs_operations)))

# --- WF: faculty_dept <- tenured_faculty x dept_lookup [on dept_code] ---
faculty_dept_bp = ctx.read_df("tenured_faculty") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  faculty_dept:      {} op(s)  [tenured_faculty x dept_lookup on dept_code]".format(
    len(faculty_dept_bp.jobs_operations)))

# --- WF: student_dept <- active_students x dept_lookup [on dept_code, param enrichment] ---
# Demonstrates param enrichment: semester from SWF params added to output
student_dept_bp = ctx.read_df("active_students", params = ["semester"], prefix = "ctx") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  student_dept:      {} op(s)  [active_students x dept_lookup on dept_code, param: semester]".format(
    len(student_dept_bp.jobs_operations)))

# --- WF: full_transcript <- student_grades x courses [on course_id, rsuffix=course_info] ---
# rsuffix needed because both sides have dept_code
full_transcript_bp = ctx.read_df("student_grades") \
    .inner_map_join(ctx.read_df("courses"), ["course_id"], rsuffix = "course_info") \
    .to_wf_spec()

utils.info("  full_transcript:   {} op(s)  [student_grades x courses on course_id, rsuffix=course_info]".format(
    len(full_transcript_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 3 — Aggregations (3 WFs)
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 3: Aggregations ---")

# --- WF: student_load <- full_transcript [group_count by student_id + student_name] ---
student_load_bp = ctx.read_df("full_transcript") \
    .group_count(["student_id", "student_name"]) \
    .to_wf_spec()

utils.info("  student_load:      {} op(s)  [group_count by student_id+student_name]".format(
    len(student_load_bp.jobs_operations)))

# --- WF: course_demand <- course_enrollment [group_count by course_id + course_name] ---
course_demand_bp = ctx.read_df("course_enrollment") \
    .group_count(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  course_demand:     {} op(s)  [group_count by course_id+course_name]".format(
    len(course_demand_bp.jobs_operations)))

# --- WF: dept_enrollment <- student_dept [group_count by dept_code + dept_name] ---
dept_enrollment_bp = ctx.read_df("student_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  dept_enrollment:   {} op(s)  [group_count by dept_code+dept_name]".format(
    len(dept_enrollment_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 4 — Final reports (2 WFs)
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 4: Final reports ---")

# --- WF: honor_roll <- student_load x active_students [on student_id, rsuffix=student_info] ---
# Diamond closure: active_students feeds both student_grades (phase 2) and honor_roll (phase 4)
# rsuffix=student_info resolves student_name conflict (both sides have student_name)
# After join, right-side cols become: student_name:student_info, gpa:student_info, dept_code:student_info, etc.
# Drop duplicate student_name:student_info, rename group:count to course_count, sort by gpa:student_info + seq_num
honor_roll_bp = ctx.read_df("student_load") \
    .inner_map_join(ctx.read_df("active_students"), ["student_id"], rsuffix = "student_info") \
    .drop_cols(["student_name:student_info"]) \
    .rename("group:count", "course_count") \
    .sort("gpa:student_info") \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  honor_roll:        {} op(s)  [student_load x active_students, drop, rename, sort, seq_num]".format(
    len(honor_roll_bp.jobs_operations)))

# --- WF: dept_report <- dept_enrollment x faculty_dept [on dept_code, rsuffix=faculty_info] ---
# rsuffix=faculty_info resolves dept_name conflict (both sides have dept_name)
# After join, right-side cols become: dept_name:faculty_info, faculty_id:faculty_info, etc.
# Drop duplicate dept_name:faculty_info, rename group:count to student_count, sort + seq_num
dept_report_bp = ctx.read_df("dept_enrollment") \
    .inner_map_join(ctx.read_df("faculty_dept"), ["dept_code"], rsuffix = "faculty_info") \
    .drop_cols(["dept_name:faculty_info"]) \
    .rename("group:count", "student_count") \
    .sort("dept_code") \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  dept_report:       {} op(s)  [dept_enrollment x faculty_dept, drop, rename, sort, seq_num]".format(
    len(dept_report_bp.jobs_operations)))

# ============================================================
# 6. Materialize SWF
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("6. Materializing SWF (mode={})".format(args.mode))
utils.info("=" * 70)

swf_id = (
    SWFBuilder(ctx, params = swf_params)
        # Phase 0 — Roots (60s buckets)
        .add_wf("students", students_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("courses", courses_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("enrollments", enrollments_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("faculty", faculty_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("departments", departments_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        # Phase 1 — Filters & transforms (60s buckets)
        .add_wf("active_students", active_students_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("upper_courses", upper_courses_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("passing_grades", passing_grades_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("tenured_faculty", tenured_faculty_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("dept_lookup", dept_lookup_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        # Phase 2 — Enrichment joins (120s buckets)
        .add_wf("student_grades", student_grades_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("course_enrollment", course_enrollment_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("faculty_dept", faculty_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("student_dept", student_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("full_transcript", full_transcript_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        # Phase 3 — Aggregations (120s buckets)
        .add_wf("student_load", student_load_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("course_demand", course_demand_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("dept_enrollment", dept_enrollment_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        # Phase 4 — Final reports (240s buckets, 240 = 2x120 = 4x60)
        .add_wf("honor_roll", honor_roll_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("dept_report", dept_report_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .materialize()
)

utils.info("  SWF result: {}".format(swf_id))

# ============================================================
# 7. Read and display results
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("7. Results")
utils.info("=" * 70)

# all output_ids to read (20 WF outputs + 2 checkpoints)
output_names = [
    "students", "courses", "enrollments", "faculty", "departments",
    "active_students", "active_ckpt", "upper_courses", "upper_ckpt",
    "passing_grades", "tenured_faculty", "dept_lookup",
    "student_grades", "course_enrollment", "faculty_dept", "student_dept", "full_transcript",
    "student_load", "course_demand", "dept_enrollment",
    "honor_roll", "dept_report",
]

results = {}
if (args.mode == "local"):
    for name in output_names:
        results[name] = ctx.read_output(name)
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

    for name in output_names:
        results[name] = cluster_services_v2.read_swf_live_output("platform", swf_id, name)

# Print results grouped by phase
utils.info("")
utils.info("  --- Phase 0: Root datasets ---")
for name in ["students", "courses", "enrollments", "faculty", "departments"]:
    r = results[name]
    utils.info("  {:20s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 1: Filters & transforms ---")
for name in ["active_students", "active_ckpt", "upper_courses", "upper_ckpt", "passing_grades", "tenured_faculty", "dept_lookup"]:
    r = results[name]
    utils.info("  {:20s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 2: Enrichment joins ---")
for name in ["student_grades", "course_enrollment", "faculty_dept", "student_dept", "full_transcript"]:
    r = results[name]
    utils.info("  {:20s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 3: Aggregations ---")
for name in ["student_load", "course_demand", "dept_enrollment"]:
    r = results[name]
    utils.info("  {:20s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 4: Final reports ---")
for name in ["honor_roll", "dept_report"]:
    r = results[name]
    utils.info("  {:20s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

# ============================================================
# Summary
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("DONE - University analytics SWF completed (20 WFs, 5 phases)")
utils.info("=" * 70)
utils.info("")
utils.info("  DAG patterns exercised:")
utils.info("    Multiple roots:  students, courses, enrollments, faculty, departments")
utils.info("    Fan-out:         students -> active_students -> student_grades + student_dept")
utils.info("    Fan-out:         courses -> upper_courses, course_enrollment, full_transcript")
utils.info("    Fan-in:          full_transcript <- student_grades + courses")
utils.info("    Diamond:         active_students -> ... -> student_load -> honor_roll <- active_students")
utils.info("    Multi-phase:     5 phases (roots -> filters -> joins -> agg -> reports)")
utils.info("    Variable buckets: 60s -> 120s -> 240s")
utils.info("")
utils.info("  API features used:")
utils.info("    ctx.from_maps()        5 root WFs with inline data")
utils.info("    ctx.read_df()          15 downstream WFs with proxy DataFrames")
utils.info("    inner_map_join         7 joins (student_grades..dept_report)")
utils.info("    rsuffix                3 joins with column conflict resolution")
utils.info("    checkpoint             2 checkpoints (active_ckpt, upper_ckpt)")
utils.info("    BVar                   min_gpa={} broadcast variable".format(bctx.min_gpa.value))
utils.info("    SWF params             semester={}, report_date={}".format(swf_params["semester"], swf_params["report_date"]))
utils.info("    param enrichment       student_dept (semester from params)")
utils.info("    group_count            3 aggregations")
utils.info("    select                 1 column pruning (dept_lookup)")
utils.info("    drop_cols + rename     2 final reports")
utils.info("    sort + add_seq_num     2 final reports with ranking")
utils.info("    not_eq_str             2 exclusion filters (passing_grades, tenured_faculty)")
utils.info("    eq_str + ge_float      active_students filter chain")
utils.info("")
utils.info("  honor_roll:     {} rows".format(results["honor_roll"].num_rows()))
utils.info("  dept_report:    {} rows".format(results["dept_report"].num_rows()))
