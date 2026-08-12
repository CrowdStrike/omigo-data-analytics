from omigo_hydra_v2 import cluster_services_v2, cluster_common_v2
from omigo_hydra_v2 import cluster_protocol_v2
from omigo_hydra_v2.cluster_services_v2 import SWFBuilder
from omigo_core import utils
import sys, os, argparse

"""Example: University analytics SWF with 200 WFs — comprehensive DAG patterns.

Demonstrates:
  1. Multiple root WFs (15 independent data sources)
  2. Complex fan-out (students feeds 15+ downstream WFs)
  3. Complex fan-in (reports read from 6+ upstream WFs)
  4. Deep diamond patterns (10 phases, 0-9)
  5. Multi-phase processing (10 phases)
  6. Variable bucket sizes (5s -> 10s -> 20s)
  7. ctx.from_maps() for root WFs with inline synthetic data
  8. ctx.read_df("upstream") for downstream WFs (proxy DataFrame, auto-resolved)
  9. inner_map_join with rsuffix for column conflict resolution
  10. Checkpoint (intermediate output_id)
  11. BVar broadcast variables (bctx.min_gpa, bctx.min_capacity, bctx.min_grant_amount)
  12. SWF params (semester, report_date, academic_year)
  13. read_df("upstream", params = ["key"], prefix = "ctx") for param enrichment
  14. group_count aggregation
  15. select (column pruning)
  16. drop_cols, rename, sort, add_seq_num for final reports
  17. Edge inference — no manual input_ids/output_ids anywhere

DAG (200 WFs, 10 phases):

  Phase 0 — Root datasets (15 WFs, data sources)
  Phase 1 — Basic filters & transforms (20 WFs)
  Phase 2 — Primary joins (25 WFs)
  Phase 3 — Secondary enrichment (25 WFs)
  Phase 4 — Aggregations (25 WFs)
  Phase 5 — Tertiary analytics (25 WFs)
  Phase 6 — Cross-domain analytics (25 WFs)
  Phase 7 — Comparative reports (15 WFs)
  Phase 8 — Deep analytics (15 WFs)
  Phase 9 — Final executive reports (10 WFs)

Run:
  python university_200wf.py --mode local
  python university_200wf.py --mode cluster
"""

# Command Line
parser = argparse.ArgumentParser(description = "SWF example: university analytics (200 WFs, 10 phases)")
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
    cluster_services_v2.init("university_200wf")
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
bctx.min_capacity = bctx.asFloat(50.0)
bctx.min_grant_amount = bctx.asFloat(200000.0)

utils.info("  min_gpa = {}".format(bctx.min_gpa.value))
utils.info("  min_capacity = {}".format(bctx.min_capacity.value))
utils.info("  min_grant_amount = {}".format(bctx.min_grant_amount.value))

# ============================================================
# 4. Define SWF params
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("4. Defining SWF params")
utils.info("=" * 70)

swf_params = {
    "semester": "Fall2026",
    "report_date": "2026-05-17",
    "academic_year": "2025-2026",
}
utils.info("  params: {}".format(swf_params))

# ============================================================
# 5. Define WF blueprints (200 WFs in 10 phases)
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("5. Defining WF blueprints (200 WFs)")
utils.info("=" * 70)

# ==============================================================================
# COLUMN TRACKING LEGEND
# Each WF definition is preceded by a comment showing its output columns.
# This is the authoritative reference for downstream WFs.
# ==============================================================================

# ------------------------------------------------------------------
# Phase 0 — Root WFs (15 WFs): inline synthetic data via ctx.from_maps()
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 0: Root datasets (15 WFs) ---")

# --- WF 1: students (root, 25 rows) ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status
students_bp = ctx.from_maps([
    {"student_id": "S001", "student_name": "Alice", "gpa": "3.8", "dept_code": "CS", "year": "4", "status": "active"},
    {"student_id": "S002", "student_name": "Bob", "gpa": "1.9", "dept_code": "MATH", "year": "3", "status": "active"},
    {"student_id": "S003", "student_name": "Charlie", "gpa": "3.5", "dept_code": "CS", "year": "2", "status": "inactive"},
    {"student_id": "S004", "student_name": "Diana", "gpa": "3.2", "dept_code": "PHYS", "year": "4", "status": "active"},
    {"student_id": "S005", "student_name": "Eve", "gpa": "3.9", "dept_code": "CS", "year": "3", "status": "active"},
    {"student_id": "S006", "student_name": "Frank", "gpa": "2.7", "dept_code": "MATH", "year": "2", "status": "active"},
    {"student_id": "S007", "student_name": "Grace", "gpa": "3.6", "dept_code": "PHYS", "year": "4", "status": "active"},
    {"student_id": "S008", "student_name": "Hank", "gpa": "2.1", "dept_code": "CHEM", "year": "1", "status": "active"},
    {"student_id": "S009", "student_name": "Ivy", "gpa": "3.3", "dept_code": "BIO", "year": "3", "status": "active"},
    {"student_id": "S010", "student_name": "Jake", "gpa": "2.5", "dept_code": "CHEM", "year": "2", "status": "inactive"},
    {"student_id": "S011", "student_name": "Karen", "gpa": "3.7", "dept_code": "BIO", "year": "4", "status": "active"},
    {"student_id": "S012", "student_name": "Leo", "gpa": "1.8", "dept_code": "MATH", "year": "1", "status": "active"},
    {"student_id": "S013", "student_name": "Mary", "gpa": "3.4", "dept_code": "CS", "year": "3", "status": "active"},
    {"student_id": "S014", "student_name": "Nathan", "gpa": "2.9", "dept_code": "PHYS", "year": "2", "status": "active"},
    {"student_id": "S015", "student_name": "Olivia", "gpa": "3.1", "dept_code": "BIO", "year": "4", "status": "active"},
    {"student_id": "S016", "student_name": "Paul", "gpa": "2.3", "dept_code": "CHEM", "year": "1", "status": "active"},
    {"student_id": "S017", "student_name": "Quinn", "gpa": "3.8", "dept_code": "CS", "year": "4", "status": "active"},
    {"student_id": "S018", "student_name": "Rachel", "gpa": "2.6", "dept_code": "MATH", "year": "2", "status": "active"},
    {"student_id": "S019", "student_name": "Sam", "gpa": "3.2", "dept_code": "PHYS", "year": "3", "status": "active"},
    {"student_id": "S020", "student_name": "Tina", "gpa": "3.0", "dept_code": "BIO", "year": "1", "status": "active"},
    {"student_id": "S021", "student_name": "Uma", "gpa": "3.6", "dept_code": "ENG", "year": "3", "status": "active"},
    {"student_id": "S022", "student_name": "Victor", "gpa": "2.4", "dept_code": "PSYCH", "year": "2", "status": "active"},
    {"student_id": "S023", "student_name": "Wendy", "gpa": "3.5", "dept_code": "ECON", "year": "4", "status": "active"},
    {"student_id": "S024", "student_name": "Xander", "gpa": "2.8", "dept_code": "ENG", "year": "1", "status": "inactive"},
    {"student_id": "S025", "student_name": "Yara", "gpa": "3.9", "dept_code": "PSYCH", "year": "3", "status": "active"},
]).to_wf_spec()

utils.info("  students:           {} op(s)  [root, 25 rows]".format(len(students_bp.jobs_operations)))

# --- WF 2: courses (root, 20 rows) ---
# OUTPUT COLS: course_id, course_name, dept_code, credits, level
courses_bp = ctx.from_maps([
    {"course_id": "C101", "course_name": "Intro to CS", "dept_code": "CS", "credits": "3", "level": "100"},
    {"course_id": "C201", "course_name": "Data Structures", "dept_code": "CS", "credits": "4", "level": "200"},
    {"course_id": "C301", "course_name": "Algorithms", "dept_code": "CS", "credits": "4", "level": "300"},
    {"course_id": "C302", "course_name": "Machine Learning", "dept_code": "CS", "credits": "4", "level": "300"},
    {"course_id": "C401", "course_name": "Advanced AI", "dept_code": "CS", "credits": "4", "level": "400"},
    {"course_id": "C110", "course_name": "Calculus I", "dept_code": "MATH", "credits": "4", "level": "100"},
    {"course_id": "C210", "course_name": "Linear Algebra", "dept_code": "MATH", "credits": "3", "level": "200"},
    {"course_id": "C310", "course_name": "Abstract Algebra", "dept_code": "MATH", "credits": "4", "level": "300"},
    {"course_id": "C120", "course_name": "General Physics", "dept_code": "PHYS", "credits": "4", "level": "100"},
    {"course_id": "C220", "course_name": "E&M Physics", "dept_code": "PHYS", "credits": "4", "level": "200"},
    {"course_id": "C320", "course_name": "Quantum Mechanics", "dept_code": "PHYS", "credits": "4", "level": "300"},
    {"course_id": "C130", "course_name": "Gen Chemistry", "dept_code": "CHEM", "credits": "4", "level": "100"},
    {"course_id": "C230", "course_name": "Organic Chemistry", "dept_code": "CHEM", "credits": "4", "level": "200"},
    {"course_id": "C140", "course_name": "Intro Biology", "dept_code": "BIO", "credits": "3", "level": "100"},
    {"course_id": "C240", "course_name": "Genetics", "dept_code": "BIO", "credits": "4", "level": "200"},
    {"course_id": "C150", "course_name": "Intro Engineering", "dept_code": "ENG", "credits": "3", "level": "100"},
    {"course_id": "C250", "course_name": "Thermodynamics", "dept_code": "ENG", "credits": "4", "level": "200"},
    {"course_id": "C160", "course_name": "Intro Psychology", "dept_code": "PSYCH", "credits": "3", "level": "100"},
    {"course_id": "C170", "course_name": "Microeconomics", "dept_code": "ECON", "credits": "3", "level": "100"},
    {"course_id": "C270", "course_name": "Econometrics", "dept_code": "ECON", "credits": "4", "level": "200"},
]).to_wf_spec()

utils.info("  courses:            {} op(s)  [root, 20 rows]".format(len(courses_bp.jobs_operations)))

# --- WF 3: enrollments (root, 50 rows) ---
# OUTPUT COLS: student_id, course_id, grade, semester
enrollments_bp = ctx.from_maps([
    {"student_id": "S001", "course_id": "C301", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S001", "course_id": "C302", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S002", "course_id": "C110", "grade": "C", "semester": "Fall2026"},
    {"student_id": "S002", "course_id": "C210", "grade": "F", "semester": "Fall2026"},
    {"student_id": "S004", "course_id": "C320", "grade": "B+", "semester": "Fall2026"},
    {"student_id": "S004", "course_id": "C120", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S005", "course_id": "C301", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S005", "course_id": "C302", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S005", "course_id": "C201", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S006", "course_id": "C110", "grade": "B", "semester": "Fall2026"},
    {"student_id": "S006", "course_id": "C210", "grade": "B-", "semester": "Fall2026"},
    {"student_id": "S007", "course_id": "C320", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S007", "course_id": "C120", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S008", "course_id": "C130", "grade": "C+", "semester": "Fall2026"},
    {"student_id": "S009", "course_id": "C140", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S009", "course_id": "C301", "grade": "B+", "semester": "Fall2026"},
    {"student_id": "S011", "course_id": "C140", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S011", "course_id": "C301", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S012", "course_id": "C110", "grade": "F", "semester": "Fall2026"},
    {"student_id": "S003", "course_id": "C201", "grade": "B", "semester": "Fall2026"},
    {"student_id": "S013", "course_id": "C301", "grade": "B+", "semester": "Fall2026"},
    {"student_id": "S013", "course_id": "C201", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S014", "course_id": "C120", "grade": "B", "semester": "Fall2026"},
    {"student_id": "S014", "course_id": "C220", "grade": "B+", "semester": "Fall2026"},
    {"student_id": "S015", "course_id": "C140", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S015", "course_id": "C240", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S016", "course_id": "C130", "grade": "C", "semester": "Fall2026"},
    {"student_id": "S017", "course_id": "C401", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S017", "course_id": "C302", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S018", "course_id": "C110", "grade": "B-", "semester": "Fall2026"},
    {"student_id": "S018", "course_id": "C210", "grade": "C+", "semester": "Fall2026"},
    {"student_id": "S019", "course_id": "C220", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S019", "course_id": "C320", "grade": "B+", "semester": "Fall2026"},
    {"student_id": "S020", "course_id": "C140", "grade": "B", "semester": "Fall2026"},
    {"student_id": "S001", "course_id": "C401", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S007", "course_id": "C220", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S011", "course_id": "C240", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S013", "course_id": "C101", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S017", "course_id": "C301", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S005", "course_id": "C401", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S021", "course_id": "C150", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S021", "course_id": "C250", "grade": "B+", "semester": "Fall2026"},
    {"student_id": "S022", "course_id": "C160", "grade": "B", "semester": "Fall2026"},
    {"student_id": "S023", "course_id": "C170", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S023", "course_id": "C270", "grade": "A-", "semester": "Fall2026"},
    {"student_id": "S024", "course_id": "C150", "grade": "C+", "semester": "Fall2026"},
    {"student_id": "S025", "course_id": "C160", "grade": "A", "semester": "Fall2026"},
    {"student_id": "S020", "course_id": "C240", "grade": "B+", "semester": "Fall2026"},
    {"student_id": "S010", "course_id": "C230", "grade": "B-", "semester": "Fall2026"},
    {"student_id": "S003", "course_id": "C301", "grade": "B", "semester": "Fall2026"},
]).to_wf_spec()

utils.info("  enrollments:        {} op(s)  [root, 50 rows]".format(len(enrollments_bp.jobs_operations)))

# --- WF 4: faculty (root, 15 rows) ---
# OUTPUT COLS: faculty_id, faculty_name, dept_code, faculty_title
faculty_bp = ctx.from_maps([
    {"faculty_id": "F01", "faculty_name": "Dr. Smith", "dept_code": "CS", "faculty_title": "Professor"},
    {"faculty_id": "F02", "faculty_name": "Dr. Johnson", "dept_code": "CS", "faculty_title": "Associate"},
    {"faculty_id": "F03", "faculty_name": "Dr. Williams", "dept_code": "MATH", "faculty_title": "Professor"},
    {"faculty_id": "F04", "faculty_name": "Dr. Brown", "dept_code": "PHYS", "faculty_title": "Adjunct"},
    {"faculty_id": "F05", "faculty_name": "Dr. Davis", "dept_code": "PHYS", "faculty_title": "Professor"},
    {"faculty_id": "F06", "faculty_name": "Dr. Miller", "dept_code": "CHEM", "faculty_title": "Associate"},
    {"faculty_id": "F07", "faculty_name": "Dr. Wilson", "dept_code": "BIO", "faculty_title": "Professor"},
    {"faculty_id": "F08", "faculty_name": "Dr. Moore", "dept_code": "MATH", "faculty_title": "Adjunct"},
    {"faculty_id": "F09", "faculty_name": "Dr. Taylor", "dept_code": "CS", "faculty_title": "Assistant"},
    {"faculty_id": "F10", "faculty_name": "Dr. Anderson", "dept_code": "PHYS", "faculty_title": "Associate"},
    {"faculty_id": "F11", "faculty_name": "Dr. Thomas", "dept_code": "BIO", "faculty_title": "Assistant"},
    {"faculty_id": "F12", "faculty_name": "Dr. Jackson", "dept_code": "CHEM", "faculty_title": "Professor"},
    {"faculty_id": "F13", "faculty_name": "Dr. Harris", "dept_code": "ENG", "faculty_title": "Professor"},
    {"faculty_id": "F14", "faculty_name": "Dr. Clark", "dept_code": "PSYCH", "faculty_title": "Associate"},
    {"faculty_id": "F15", "faculty_name": "Dr. Lewis", "dept_code": "ECON", "faculty_title": "Professor"},
]).to_wf_spec()

utils.info("  faculty:            {} op(s)  [root, 15 rows]".format(len(faculty_bp.jobs_operations)))

# --- WF 5: departments (root, 8 rows) ---
# OUTPUT COLS: dept_code, dept_name, building, budget
departments_bp = ctx.from_maps([
    {"dept_code": "CS", "dept_name": "Computer Science", "building": "Turing Hall", "budget": "5000000"},
    {"dept_code": "MATH", "dept_name": "Mathematics", "building": "Euler Hall", "budget": "3000000"},
    {"dept_code": "PHYS", "dept_name": "Physics", "building": "Newton Hall", "budget": "4000000"},
    {"dept_code": "CHEM", "dept_name": "Chemistry", "building": "Curie Hall", "budget": "3500000"},
    {"dept_code": "BIO", "dept_name": "Biology", "building": "Darwin Hall", "budget": "3200000"},
    {"dept_code": "ENG", "dept_name": "Engineering", "building": "Edison Hall", "budget": "6000000"},
    {"dept_code": "PSYCH", "dept_name": "Psychology", "building": "Freud Hall", "budget": "2500000"},
    {"dept_code": "ECON", "dept_name": "Economics", "building": "Keynes Hall", "budget": "2800000"},
]).to_wf_spec()

utils.info("  departments:        {} op(s)  [root, 8 rows]".format(len(departments_bp.jobs_operations)))

# --- WF 6: classrooms (root, 12 rows) ---
# OUTPUT COLS: room_id, building, capacity, room_type
classrooms_bp = ctx.from_maps([
    {"room_id": "R101", "building": "Turing Hall", "capacity": "80", "room_type": "lecture"},
    {"room_id": "R102", "building": "Turing Hall", "capacity": "30", "room_type": "lab"},
    {"room_id": "R201", "building": "Euler Hall", "capacity": "100", "room_type": "lecture"},
    {"room_id": "R202", "building": "Euler Hall", "capacity": "40", "room_type": "seminar"},
    {"room_id": "R301", "building": "Newton Hall", "capacity": "60", "room_type": "lecture"},
    {"room_id": "R302", "building": "Newton Hall", "capacity": "25", "room_type": "lab"},
    {"room_id": "R401", "building": "Curie Hall", "capacity": "50", "room_type": "lab"},
    {"room_id": "R501", "building": "Darwin Hall", "capacity": "70", "room_type": "lecture"},
    {"room_id": "R502", "building": "Darwin Hall", "capacity": "35", "room_type": "lab"},
    {"room_id": "R601", "building": "Edison Hall", "capacity": "120", "room_type": "lecture"},
    {"room_id": "R701", "building": "Freud Hall", "capacity": "55", "room_type": "seminar"},
    {"room_id": "R801", "building": "Keynes Hall", "capacity": "65", "room_type": "lecture"},
]).to_wf_spec()

utils.info("  classrooms:         {} op(s)  [root, 12 rows]".format(len(classrooms_bp.jobs_operations)))

# --- WF 7: schedules (root, 25 rows) ---
# OUTPUT COLS: schedule_id, course_id, room_id, faculty_id, time_slot, semester
schedules_bp = ctx.from_maps([
    {"schedule_id": "SCH001", "course_id": "C301", "room_id": "R101", "faculty_id": "F01", "time_slot": "MWF 9-10", "semester": "Fall2026"},
    {"schedule_id": "SCH002", "course_id": "C302", "room_id": "R101", "faculty_id": "F01", "time_slot": "TTH 10-12", "semester": "Fall2026"},
    {"schedule_id": "SCH003", "course_id": "C201", "room_id": "R102", "faculty_id": "F02", "time_slot": "MWF 2-3", "semester": "Fall2026"},
    {"schedule_id": "SCH004", "course_id": "C110", "room_id": "R201", "faculty_id": "F03", "time_slot": "MWF 9-10", "semester": "Fall2026"},
    {"schedule_id": "SCH005", "course_id": "C210", "room_id": "R202", "faculty_id": "F03", "time_slot": "TTH 1-2", "semester": "Fall2026"},
    {"schedule_id": "SCH006", "course_id": "C320", "room_id": "R301", "faculty_id": "F05", "time_slot": "MWF 11-12", "semester": "Fall2026"},
    {"schedule_id": "SCH007", "course_id": "C120", "room_id": "R301", "faculty_id": "F04", "time_slot": "TTH 9-11", "semester": "Fall2026"},
    {"schedule_id": "SCH008", "course_id": "C130", "room_id": "R401", "faculty_id": "F06", "time_slot": "MWF 2-4", "semester": "Fall2026"},
    {"schedule_id": "SCH009", "course_id": "C140", "room_id": "R501", "faculty_id": "F07", "time_slot": "TTH 10-11", "semester": "Fall2026"},
    {"schedule_id": "SCH010", "course_id": "C101", "room_id": "R101", "faculty_id": "F09", "time_slot": "MWF 1-2", "semester": "Fall2026"},
    {"schedule_id": "SCH011", "course_id": "C401", "room_id": "R101", "faculty_id": "F01", "time_slot": "TTH 2-4", "semester": "Fall2026"},
    {"schedule_id": "SCH012", "course_id": "C310", "room_id": "R201", "faculty_id": "F08", "time_slot": "MWF 3-4", "semester": "Fall2026"},
    {"schedule_id": "SCH013", "course_id": "C220", "room_id": "R301", "faculty_id": "F10", "time_slot": "TTH 1-3", "semester": "Fall2026"},
    {"schedule_id": "SCH014", "course_id": "C230", "room_id": "R401", "faculty_id": "F12", "time_slot": "MWF 10-12", "semester": "Fall2026"},
    {"schedule_id": "SCH015", "course_id": "C240", "room_id": "R501", "faculty_id": "F11", "time_slot": "TTH 11-12", "semester": "Fall2026"},
    {"schedule_id": "SCH016", "course_id": "C301", "room_id": "R102", "faculty_id": "F02", "time_slot": "F 2-5", "semester": "Fall2026"},
    {"schedule_id": "SCH017", "course_id": "C210", "room_id": "R202", "faculty_id": "F08", "time_slot": "W 3-5", "semester": "Fall2026"},
    {"schedule_id": "SCH018", "course_id": "C120", "room_id": "R302", "faculty_id": "F10", "time_slot": "T 2-5", "semester": "Fall2026"},
    {"schedule_id": "SCH019", "course_id": "C140", "room_id": "R502", "faculty_id": "F11", "time_slot": "TH 2-5", "semester": "Fall2026"},
    {"schedule_id": "SCH020", "course_id": "C401", "room_id": "R601", "faculty_id": "F09", "time_slot": "M 6-9", "semester": "Fall2026"},
    {"schedule_id": "SCH021", "course_id": "C150", "room_id": "R601", "faculty_id": "F13", "time_slot": "MWF 10-11", "semester": "Fall2026"},
    {"schedule_id": "SCH022", "course_id": "C250", "room_id": "R601", "faculty_id": "F13", "time_slot": "TTH 2-4", "semester": "Fall2026"},
    {"schedule_id": "SCH023", "course_id": "C160", "room_id": "R701", "faculty_id": "F14", "time_slot": "MWF 1-2", "semester": "Fall2026"},
    {"schedule_id": "SCH024", "course_id": "C170", "room_id": "R801", "faculty_id": "F15", "time_slot": "TTH 9-10", "semester": "Fall2026"},
    {"schedule_id": "SCH025", "course_id": "C270", "room_id": "R801", "faculty_id": "F15", "time_slot": "MWF 3-4", "semester": "Fall2026"},
]).to_wf_spec()

utils.info("  schedules:          {} op(s)  [root, 25 rows]".format(len(schedules_bp.jobs_operations)))

# --- WF 8: prerequisites (root, 15 rows) ---
# OUTPUT COLS: course_id, prereq_id
prerequisites_bp = ctx.from_maps([
    {"course_id": "C201", "prereq_id": "C101"},
    {"course_id": "C301", "prereq_id": "C201"},
    {"course_id": "C302", "prereq_id": "C301"},
    {"course_id": "C401", "prereq_id": "C302"},
    {"course_id": "C210", "prereq_id": "C110"},
    {"course_id": "C310", "prereq_id": "C210"},
    {"course_id": "C220", "prereq_id": "C120"},
    {"course_id": "C320", "prereq_id": "C220"},
    {"course_id": "C230", "prereq_id": "C130"},
    {"course_id": "C240", "prereq_id": "C140"},
    {"course_id": "C302", "prereq_id": "C210"},
    {"course_id": "C401", "prereq_id": "C310"},
    {"course_id": "C250", "prereq_id": "C150"},
    {"course_id": "C270", "prereq_id": "C170"},
    {"course_id": "C320", "prereq_id": "C120"},
]).to_wf_spec()

utils.info("  prerequisites:      {} op(s)  [root, 15 rows]".format(len(prerequisites_bp.jobs_operations)))

# --- WF 9: research_grants (root, 12 rows) ---
# OUTPUT COLS: grant_id, faculty_id, grant_amount, grant_title, grant_year
research_grants_bp = ctx.from_maps([
    {"grant_id": "G001", "faculty_id": "F01", "grant_amount": "500000", "grant_title": "AI Safety Research", "grant_year": "2026"},
    {"grant_id": "G002", "faculty_id": "F01", "grant_amount": "300000", "grant_title": "ML Optimization", "grant_year": "2026"},
    {"grant_id": "G003", "faculty_id": "F03", "grant_amount": "250000", "grant_title": "Number Theory Apps", "grant_year": "2026"},
    {"grant_id": "G004", "faculty_id": "F05", "grant_amount": "600000", "grant_title": "Quantum Computing", "grant_year": "2026"},
    {"grant_id": "G005", "faculty_id": "F07", "grant_amount": "400000", "grant_title": "Genomics Research", "grant_year": "2026"},
    {"grant_id": "G006", "faculty_id": "F12", "grant_amount": "350000", "grant_title": "Green Chemistry", "grant_year": "2026"},
    {"grant_id": "G007", "faculty_id": "F02", "grant_amount": "200000", "grant_title": "Data Mining", "grant_year": "2026"},
    {"grant_id": "G008", "faculty_id": "F10", "grant_amount": "450000", "grant_title": "Particle Physics", "grant_year": "2026"},
    {"grant_id": "G009", "faculty_id": "F13", "grant_amount": "550000", "grant_title": "Renewable Energy Systems", "grant_year": "2026"},
    {"grant_id": "G010", "faculty_id": "F14", "grant_amount": "180000", "grant_title": "Cognitive Behavior Study", "grant_year": "2026"},
    {"grant_id": "G011", "faculty_id": "F15", "grant_amount": "320000", "grant_title": "Market Dynamics Model", "grant_year": "2026"},
    {"grant_id": "G012", "faculty_id": "F09", "grant_amount": "280000", "grant_title": "Neural Architecture Search", "grant_year": "2026"},
]).to_wf_spec()

utils.info("  research_grants:    {} op(s)  [root, 12 rows]".format(len(research_grants_bp.jobs_operations)))

# --- WF 10: publications (root, 20 rows) ---
# OUTPUT COLS: pub_id, faculty_id, pub_title, venue, pub_year
publications_bp = ctx.from_maps([
    {"pub_id": "P001", "faculty_id": "F01", "pub_title": "Deep Learning Advances", "venue": "NeurIPS", "pub_year": "2026"},
    {"pub_id": "P002", "faculty_id": "F01", "pub_title": "Transformer Models", "venue": "ICML", "pub_year": "2026"},
    {"pub_id": "P003", "faculty_id": "F02", "pub_title": "Big Data Analytics", "venue": "KDD", "pub_year": "2026"},
    {"pub_id": "P004", "faculty_id": "F03", "pub_title": "Prime Number Theorems", "venue": "Annals of Math", "pub_year": "2026"},
    {"pub_id": "P005", "faculty_id": "F03", "pub_title": "Algebraic Structures", "venue": "J. Algebra", "pub_year": "2026"},
    {"pub_id": "P006", "faculty_id": "F05", "pub_title": "Quantum Entanglement", "venue": "Nature Physics", "pub_year": "2026"},
    {"pub_id": "P007", "faculty_id": "F05", "pub_title": "Quantum Algorithms", "venue": "Physical Review", "pub_year": "2026"},
    {"pub_id": "P008", "faculty_id": "F07", "pub_title": "Gene Expression", "venue": "Nature", "pub_year": "2026"},
    {"pub_id": "P009", "faculty_id": "F07", "pub_title": "Evolution Dynamics", "venue": "Science", "pub_year": "2026"},
    {"pub_id": "P010", "faculty_id": "F12", "pub_title": "Sustainable Catalysis", "venue": "JACS", "pub_year": "2026"},
    {"pub_id": "P011", "faculty_id": "F10", "pub_title": "Standard Model Tests", "venue": "CERN Reports", "pub_year": "2026"},
    {"pub_id": "P012", "faculty_id": "F09", "pub_title": "Neural Architecture Search", "venue": "ICLR", "pub_year": "2026"},
    {"pub_id": "P013", "faculty_id": "F11", "pub_title": "Microbiome Analysis", "venue": "Cell", "pub_year": "2026"},
    {"pub_id": "P014", "faculty_id": "F06", "pub_title": "Polymer Chemistry", "venue": "Macromolecules", "pub_year": "2026"},
    {"pub_id": "P015", "faculty_id": "F01", "pub_title": "Reinforcement Learning", "venue": "AAAI", "pub_year": "2026"},
    {"pub_id": "P016", "faculty_id": "F13", "pub_title": "Smart Grid Optimization", "venue": "IEEE Trans Energy", "pub_year": "2026"},
    {"pub_id": "P017", "faculty_id": "F13", "pub_title": "Wind Turbine Efficiency", "venue": "Renewable Energy", "pub_year": "2026"},
    {"pub_id": "P018", "faculty_id": "F14", "pub_title": "Attention and Memory", "venue": "Psych Review", "pub_year": "2026"},
    {"pub_id": "P019", "faculty_id": "F15", "pub_title": "Inflation Dynamics", "venue": "AER", "pub_year": "2026"},
    {"pub_id": "P020", "faculty_id": "F15", "pub_title": "Trade Policy Models", "venue": "Econometrica", "pub_year": "2026"},
]).to_wf_spec()

utils.info("  publications:       {} op(s)  [root, 20 rows]".format(len(publications_bp.jobs_operations)))

# --- WF 11: clubs (root, 12 rows) ---
# OUTPUT COLS: club_id, club_name, dept_code, club_budget
clubs_bp = ctx.from_maps([
    {"club_id": "CL01", "club_name": "CS Club", "dept_code": "CS", "club_budget": "5000"},
    {"club_id": "CL02", "club_name": "Math Society", "dept_code": "MATH", "club_budget": "3000"},
    {"club_id": "CL03", "club_name": "Physics Society", "dept_code": "PHYS", "club_budget": "4000"},
    {"club_id": "CL04", "club_name": "Chemistry Club", "dept_code": "CHEM", "club_budget": "3500"},
    {"club_id": "CL05", "club_name": "Biology Club", "dept_code": "BIO", "club_budget": "3200"},
    {"club_id": "CL06", "club_name": "Robotics Club", "dept_code": "CS", "club_budget": "8000"},
    {"club_id": "CL07", "club_name": "Debate Society", "dept_code": "PSYCH", "club_budget": "2000"},
    {"club_id": "CL08", "club_name": "Astronomy Club", "dept_code": "PHYS", "club_budget": "4500"},
    {"club_id": "CL09", "club_name": "Research Society", "dept_code": "CS", "club_budget": "6000"},
    {"club_id": "CL10", "club_name": "Environmental Club", "dept_code": "BIO", "club_budget": "4000"},
    {"club_id": "CL11", "club_name": "Engineering Guild", "dept_code": "ENG", "club_budget": "7000"},
    {"club_id": "CL12", "club_name": "Investment Club", "dept_code": "ECON", "club_budget": "5500"},
]).to_wf_spec()

utils.info("  clubs:              {} op(s)  [root, 12 rows]".format(len(clubs_bp.jobs_operations)))

# --- WF 12: club_members (root, 30 rows) ---
# OUTPUT COLS: student_id, club_id, member_role
club_members_bp = ctx.from_maps([
    {"student_id": "S001", "club_id": "CL01", "member_role": "president"},
    {"student_id": "S001", "club_id": "CL09", "member_role": "member"},
    {"student_id": "S005", "club_id": "CL01", "member_role": "vice-president"},
    {"student_id": "S005", "club_id": "CL06", "member_role": "member"},
    {"student_id": "S013", "club_id": "CL01", "member_role": "member"},
    {"student_id": "S017", "club_id": "CL01", "member_role": "treasurer"},
    {"student_id": "S017", "club_id": "CL09", "member_role": "president"},
    {"student_id": "S002", "club_id": "CL02", "member_role": "member"},
    {"student_id": "S006", "club_id": "CL02", "member_role": "president"},
    {"student_id": "S004", "club_id": "CL03", "member_role": "member"},
    {"student_id": "S007", "club_id": "CL03", "member_role": "president"},
    {"student_id": "S007", "club_id": "CL08", "member_role": "member"},
    {"student_id": "S019", "club_id": "CL03", "member_role": "member"},
    {"student_id": "S008", "club_id": "CL04", "member_role": "member"},
    {"student_id": "S016", "club_id": "CL04", "member_role": "vice-president"},
    {"student_id": "S009", "club_id": "CL05", "member_role": "president"},
    {"student_id": "S011", "club_id": "CL05", "member_role": "member"},
    {"student_id": "S015", "club_id": "CL05", "member_role": "treasurer"},
    {"student_id": "S020", "club_id": "CL05", "member_role": "member"},
    {"student_id": "S011", "club_id": "CL10", "member_role": "member"},
    {"student_id": "S013", "club_id": "CL06", "member_role": "member"},
    {"student_id": "S005", "club_id": "CL09", "member_role": "member"},
    {"student_id": "S014", "club_id": "CL03", "member_role": "treasurer"},
    {"student_id": "S018", "club_id": "CL02", "member_role": "member"},
    {"student_id": "S019", "club_id": "CL08", "member_role": "president"},
    {"student_id": "S021", "club_id": "CL11", "member_role": "president"},
    {"student_id": "S024", "club_id": "CL11", "member_role": "member"},
    {"student_id": "S023", "club_id": "CL12", "member_role": "president"},
    {"student_id": "S022", "club_id": "CL07", "member_role": "member"},
    {"student_id": "S025", "club_id": "CL07", "member_role": "president"},
]).to_wf_spec()

utils.info("  club_members:       {} op(s)  [root, 30 rows]".format(len(club_members_bp.jobs_operations)))

# --- WF 13: advisors (root, 20 rows) ---
# OUTPUT COLS: advisor_id, faculty_id, student_id, advisor_type, start_year
advisors_bp = ctx.from_maps([
    {"advisor_id": "ADV01", "faculty_id": "F01", "student_id": "S001", "advisor_type": "research", "start_year": "2024"},
    {"advisor_id": "ADV02", "faculty_id": "F01", "student_id": "S005", "advisor_type": "research", "start_year": "2025"},
    {"advisor_id": "ADV03", "faculty_id": "F01", "student_id": "S017", "advisor_type": "academic", "start_year": "2023"},
    {"advisor_id": "ADV04", "faculty_id": "F02", "student_id": "S013", "advisor_type": "academic", "start_year": "2025"},
    {"advisor_id": "ADV05", "faculty_id": "F03", "student_id": "S002", "advisor_type": "academic", "start_year": "2024"},
    {"advisor_id": "ADV06", "faculty_id": "F03", "student_id": "S006", "advisor_type": "academic", "start_year": "2025"},
    {"advisor_id": "ADV07", "faculty_id": "F05", "student_id": "S004", "advisor_type": "research", "start_year": "2024"},
    {"advisor_id": "ADV08", "faculty_id": "F05", "student_id": "S007", "advisor_type": "research", "start_year": "2023"},
    {"advisor_id": "ADV09", "faculty_id": "F07", "student_id": "S009", "advisor_type": "research", "start_year": "2025"},
    {"advisor_id": "ADV10", "faculty_id": "F07", "student_id": "S011", "advisor_type": "academic", "start_year": "2023"},
    {"advisor_id": "ADV11", "faculty_id": "F07", "student_id": "S015", "advisor_type": "academic", "start_year": "2024"},
    {"advisor_id": "ADV12", "faculty_id": "F10", "student_id": "S014", "advisor_type": "academic", "start_year": "2025"},
    {"advisor_id": "ADV13", "faculty_id": "F10", "student_id": "S019", "advisor_type": "research", "start_year": "2024"},
    {"advisor_id": "ADV14", "faculty_id": "F12", "student_id": "S008", "advisor_type": "academic", "start_year": "2026"},
    {"advisor_id": "ADV15", "faculty_id": "F12", "student_id": "S016", "advisor_type": "academic", "start_year": "2026"},
    {"advisor_id": "ADV16", "faculty_id": "F13", "student_id": "S021", "advisor_type": "research", "start_year": "2025"},
    {"advisor_id": "ADV17", "faculty_id": "F13", "student_id": "S024", "advisor_type": "academic", "start_year": "2026"},
    {"advisor_id": "ADV18", "faculty_id": "F14", "student_id": "S022", "advisor_type": "academic", "start_year": "2025"},
    {"advisor_id": "ADV19", "faculty_id": "F14", "student_id": "S025", "advisor_type": "research", "start_year": "2024"},
    {"advisor_id": "ADV20", "faculty_id": "F15", "student_id": "S023", "advisor_type": "research", "start_year": "2024"},
]).to_wf_spec()

utils.info("  advisors:           {} op(s)  [root, 20 rows]".format(len(advisors_bp.jobs_operations)))

# --- WF 14: internships (root, 18 rows) ---
# OUTPUT COLS: internship_id, student_id, company_name, intern_role, intern_semester, intern_stipend
internships_bp = ctx.from_maps([
    {"internship_id": "INT01", "student_id": "S001", "company_name": "Google", "intern_role": "SWE Intern", "intern_semester": "Summer2026", "intern_stipend": "9000"},
    {"internship_id": "INT02", "student_id": "S005", "company_name": "Meta", "intern_role": "ML Intern", "intern_semester": "Summer2026", "intern_stipend": "8500"},
    {"internship_id": "INT03", "student_id": "S017", "company_name": "Apple", "intern_role": "SWE Intern", "intern_semester": "Summer2026", "intern_stipend": "9200"},
    {"internship_id": "INT04", "student_id": "S013", "company_name": "Amazon", "intern_role": "Data Intern", "intern_semester": "Summer2026", "intern_stipend": "8000"},
    {"internship_id": "INT05", "student_id": "S007", "company_name": "SpaceX", "intern_role": "Physics Intern", "intern_semester": "Summer2026", "intern_stipend": "8800"},
    {"internship_id": "INT06", "student_id": "S004", "company_name": "NASA", "intern_role": "Research Intern", "intern_semester": "Summer2026", "intern_stipend": "7500"},
    {"internship_id": "INT07", "student_id": "S011", "company_name": "Genentech", "intern_role": "Bio Intern", "intern_semester": "Summer2026", "intern_stipend": "7800"},
    {"internship_id": "INT08", "student_id": "S009", "company_name": "Pfizer", "intern_role": "Research Intern", "intern_semester": "Summer2026", "intern_stipend": "7200"},
    {"internship_id": "INT09", "student_id": "S003", "company_name": "Microsoft", "intern_role": "SWE Intern", "intern_semester": "Summer2026", "intern_stipend": "8700"},
    {"internship_id": "INT10", "student_id": "S015", "company_name": "Amgen", "intern_role": "Lab Intern", "intern_semester": "Summer2026", "intern_stipend": "7000"},
    {"internship_id": "INT11", "student_id": "S021", "company_name": "Tesla", "intern_role": "Eng Intern", "intern_semester": "Summer2026", "intern_stipend": "9100"},
    {"internship_id": "INT12", "student_id": "S023", "company_name": "Goldman Sachs", "intern_role": "Finance Intern", "intern_semester": "Summer2026", "intern_stipend": "10000"},
    {"internship_id": "INT13", "student_id": "S025", "company_name": "McKinsey", "intern_role": "Analyst Intern", "intern_semester": "Summer2026", "intern_stipend": "9500"},
    {"internship_id": "INT14", "student_id": "S022", "company_name": "Mayo Clinic", "intern_role": "Psych Intern", "intern_semester": "Summer2026", "intern_stipend": "6500"},
    {"internship_id": "INT15", "student_id": "S019", "company_name": "Lockheed Martin", "intern_role": "Physics Intern", "intern_semester": "Summer2026", "intern_stipend": "8200"},
    {"internship_id": "INT16", "student_id": "S006", "company_name": "Citadel", "intern_role": "Quant Intern", "intern_semester": "Summer2026", "intern_stipend": "11000"},
    {"internship_id": "INT17", "student_id": "S014", "company_name": "Boeing", "intern_role": "Eng Intern", "intern_semester": "Summer2026", "intern_stipend": "7800"},
    {"internship_id": "INT18", "student_id": "S020", "company_name": "Merck", "intern_role": "Bio Intern", "intern_semester": "Summer2026", "intern_stipend": "7100"},
]).to_wf_spec()

utils.info("  internships:        {} op(s)  [root, 18 rows]".format(len(internships_bp.jobs_operations)))

# --- WF 15: library_books (root, 20 rows) ---
# OUTPUT COLS: book_id, book_title, book_author, dept_code, copies_total, copies_available
library_books_bp = ctx.from_maps([
    {"book_id": "LB001", "book_title": "Intro to Algorithms", "book_author": "Cormen", "dept_code": "CS", "copies_total": "10", "copies_available": "3"},
    {"book_id": "LB002", "book_title": "Design Patterns", "book_author": "Gamma", "dept_code": "CS", "copies_total": "8", "copies_available": "5"},
    {"book_id": "LB003", "book_title": "Deep Learning", "book_author": "Goodfellow", "dept_code": "CS", "copies_total": "6", "copies_available": "1"},
    {"book_id": "LB004", "book_title": "Calculus", "book_author": "Stewart", "dept_code": "MATH", "copies_total": "15", "copies_available": "7"},
    {"book_id": "LB005", "book_title": "Linear Algebra Done Right", "book_author": "Axler", "dept_code": "MATH", "copies_total": "7", "copies_available": "4"},
    {"book_id": "LB006", "book_title": "Abstract Algebra", "book_author": "Dummit", "dept_code": "MATH", "copies_total": "5", "copies_available": "2"},
    {"book_id": "LB007", "book_title": "Classical Mechanics", "book_author": "Goldstein", "dept_code": "PHYS", "copies_total": "8", "copies_available": "3"},
    {"book_id": "LB008", "book_title": "Quantum Mechanics", "book_author": "Griffiths", "dept_code": "PHYS", "copies_total": "9", "copies_available": "2"},
    {"book_id": "LB009", "book_title": "E&M Theory", "book_author": "Jackson", "dept_code": "PHYS", "copies_total": "6", "copies_available": "4"},
    {"book_id": "LB010", "book_title": "Organic Chemistry", "book_author": "Clayden", "dept_code": "CHEM", "copies_total": "12", "copies_available": "6"},
    {"book_id": "LB011", "book_title": "Inorganic Chemistry", "book_author": "Miessler", "dept_code": "CHEM", "copies_total": "7", "copies_available": "5"},
    {"book_id": "LB012", "book_title": "Molecular Biology", "book_author": "Watson", "dept_code": "BIO", "copies_total": "10", "copies_available": "4"},
    {"book_id": "LB013", "book_title": "Genetics", "book_author": "Hartwell", "dept_code": "BIO", "copies_total": "8", "copies_available": "3"},
    {"book_id": "LB014", "book_title": "Ecology", "book_author": "Begon", "dept_code": "BIO", "copies_total": "6", "copies_available": "6"},
    {"book_id": "LB015", "book_title": "Statics and Dynamics", "book_author": "Meriam", "dept_code": "ENG", "copies_total": "9", "copies_available": "5"},
    {"book_id": "LB016", "book_title": "Thermodynamics", "book_author": "Cengel", "dept_code": "ENG", "copies_total": "7", "copies_available": "2"},
    {"book_id": "LB017", "book_title": "Cognitive Psychology", "book_author": "Sternberg", "dept_code": "PSYCH", "copies_total": "11", "copies_available": "8"},
    {"book_id": "LB018", "book_title": "Social Psychology", "book_author": "Aronson", "dept_code": "PSYCH", "copies_total": "6", "copies_available": "4"},
    {"book_id": "LB019", "book_title": "Microeconomics", "book_author": "Varian", "dept_code": "ECON", "copies_total": "10", "copies_available": "5"},
    {"book_id": "LB020", "book_title": "Macroeconomics", "book_author": "Mankiw", "dept_code": "ECON", "copies_total": "12", "copies_available": "7"},
]).to_wf_spec()

utils.info("  library_books:      {} op(s)  [root, 20 rows]".format(len(library_books_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 1 — Basic filters & transforms (20 WFs): WFs 16-35
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 1: Basic filters & transforms (20 WFs) ---")

# --- WF 16: active_students <- students [filter status=active, gpa >= min_gpa] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status
active_students_bp = ctx.read_df("students") \
    .eq_str("status", "active") \
    .ge_float("gpa", bctx.min_gpa.value) \
    .checkpoint("active_ckpt") \
    .to_wf_spec()

utils.info("  active_students:    {} op(s)".format(len(active_students_bp.jobs_operations)))

# --- WF 17: upper_courses <- courses [filter level >= 300] ---
# OUTPUT COLS: course_id, course_name, dept_code, credits, level
upper_courses_bp = ctx.read_df("courses") \
    .ge_float("level", 300.0) \
    .checkpoint("upper_ckpt") \
    .to_wf_spec()

utils.info("  upper_courses:      {} op(s)".format(len(upper_courses_bp.jobs_operations)))

# --- WF 18: passing_grades <- enrollments [filter grade != F] ---
# OUTPUT COLS: student_id, course_id, grade, semester
passing_grades_bp = ctx.read_df("enrollments") \
    .not_eq_str("grade", "F") \
    .to_wf_spec()

utils.info("  passing_grades:     {} op(s)".format(len(passing_grades_bp.jobs_operations)))

# --- WF 19: tenured_faculty <- faculty [filter faculty_title != Adjunct] ---
# OUTPUT COLS: faculty_id, faculty_name, dept_code, faculty_title
tenured_faculty_bp = ctx.read_df("faculty") \
    .not_eq_str("faculty_title", "Adjunct") \
    .to_wf_spec()

utils.info("  tenured_faculty:    {} op(s)".format(len(tenured_faculty_bp.jobs_operations)))

# --- WF 20: dept_lookup <- departments [select dept_code, dept_name] ---
# OUTPUT COLS: dept_code, dept_name
dept_lookup_bp = ctx.read_df("departments") \
    .select(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  dept_lookup:        {} op(s)".format(len(dept_lookup_bp.jobs_operations)))

# --- WF 21: large_rooms <- classrooms [filter capacity >= min_capacity] ---
# OUTPUT COLS: room_id, building, capacity, room_type
large_rooms_bp = ctx.read_df("classrooms") \
    .ge_float("capacity", bctx.min_capacity.value) \
    .to_wf_spec()

utils.info("  large_rooms:        {} op(s)".format(len(large_rooms_bp.jobs_operations)))

# --- WF 22: fall_schedules <- schedules [filter semester=Fall2026] ---
# OUTPUT COLS: schedule_id, course_id, room_id, faculty_id, time_slot, semester
fall_schedules_bp = ctx.read_df("schedules") \
    .eq_str("semester", "Fall2026") \
    .to_wf_spec()

utils.info("  fall_schedules:     {} op(s)".format(len(fall_schedules_bp.jobs_operations)))

# --- WF 23: course_id_name <- courses [select course_id, course_name] ---
# OUTPUT COLS: course_id, course_name
course_id_name_bp = ctx.read_df("courses") \
    .select(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  course_id_name:     {} op(s)".format(len(course_id_name_bp.jobs_operations)))

# --- WF 24: student_id_name <- students [select student_id, student_name] ---
# OUTPUT COLS: student_id, student_name
student_id_name_bp = ctx.read_df("students") \
    .select(["student_id", "student_name"]) \
    .to_wf_spec()

utils.info("  student_id_name:    {} op(s)".format(len(student_id_name_bp.jobs_operations)))

# --- WF 25: faculty_id_name <- faculty [select faculty_id, faculty_name] ---
# OUTPUT COLS: faculty_id, faculty_name
faculty_id_name_bp = ctx.read_df("faculty") \
    .select(["faculty_id", "faculty_name"]) \
    .to_wf_spec()

utils.info("  faculty_id_name:    {} op(s)".format(len(faculty_id_name_bp.jobs_operations)))

# --- WF 26: high_gpa_students <- students [filter gpa >= 3.5] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status
high_gpa_students_bp = ctx.read_df("students") \
    .ge_float("gpa", 3.5) \
    .to_wf_spec()

utils.info("  high_gpa_students:  {} op(s)".format(len(high_gpa_students_bp.jobs_operations)))

# --- WF 27: senior_students <- students [filter year=4] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status
senior_students_bp = ctx.read_df("students") \
    .eq_str("year", "4") \
    .to_wf_spec()

utils.info("  senior_students:    {} op(s)".format(len(senior_students_bp.jobs_operations)))

# --- WF 28: dept_budget_lookup <- departments [select dept_code, budget] ---
# OUTPUT COLS: dept_code, budget
dept_budget_lookup_bp = ctx.read_df("departments") \
    .select(["dept_code", "budget"]) \
    .to_wf_spec()

utils.info("  dept_budget_lookup: {} op(s)".format(len(dept_budget_lookup_bp.jobs_operations)))

# --- WF 29: club_lookup <- clubs [select club_id, club_name] ---
# OUTPUT COLS: club_id, club_name
club_lookup_bp = ctx.read_df("clubs") \
    .select(["club_id", "club_name"]) \
    .to_wf_spec()

utils.info("  club_lookup:        {} op(s)".format(len(club_lookup_bp.jobs_operations)))

# --- WF 30: large_grants <- research_grants [filter grant_amount >= min_grant_amount] ---
# OUTPUT COLS: grant_id, faculty_id, grant_amount, grant_title, grant_year
large_grants_bp = ctx.read_df("research_grants") \
    .ge_float("grant_amount", bctx.min_grant_amount.value) \
    .to_wf_spec()

utils.info("  large_grants:       {} op(s)".format(len(large_grants_bp.jobs_operations)))

# --- WF 31: cs_students <- students [filter dept_code=CS] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status
cs_students_bp = ctx.read_df("students") \
    .eq_str("dept_code", "CS") \
    .to_wf_spec()

utils.info("  cs_students:        {} op(s)".format(len(cs_students_bp.jobs_operations)))

# --- WF 32: recent_pubs <- publications [filter pub_year=2026] ---
# OUTPUT COLS: pub_id, faculty_id, pub_title, venue, pub_year
recent_pubs_bp = ctx.read_df("publications") \
    .eq_str("pub_year", "2026") \
    .to_wf_spec()

utils.info("  recent_pubs:        {} op(s)".format(len(recent_pubs_bp.jobs_operations)))

# --- WF 33: research_advisors <- advisors [filter advisor_type=research] ---
# OUTPUT COLS: advisor_id, faculty_id, student_id, advisor_type, start_year
research_advisors_bp = ctx.read_df("advisors") \
    .eq_str("advisor_type", "research") \
    .to_wf_spec()

utils.info("  research_advisors:  {} op(s)".format(len(research_advisors_bp.jobs_operations)))

# --- WF 34: high_stipend_interns <- internships [filter intern_stipend >= 8000] ---
# OUTPUT COLS: internship_id, student_id, company_name, intern_role, intern_semester, intern_stipend
high_stipend_interns_bp = ctx.read_df("internships") \
    .ge_float("intern_stipend", 8000.0) \
    .to_wf_spec()

utils.info("  high_stipend_interns: {} op(s)".format(len(high_stipend_interns_bp.jobs_operations)))

# --- WF 35: low_avail_books <- library_books [filter copies_available < 5] ---
# OUTPUT COLS: book_id, book_title, book_author, dept_code, copies_total, copies_available
low_avail_books_bp = ctx.read_df("library_books") \
    .lt_int("copies_available", 5) \
    .to_wf_spec()

utils.info("  low_avail_books:    {} op(s)".format(len(low_avail_books_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 2 — Primary joins (25 WFs): WFs 36-60
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 2: Primary joins (25 WFs) ---")

# --- WF 36: student_grades <- active_students x passing_grades [on student_id] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester
student_grades_bp = ctx.read_df("active_students") \
    .inner_map_join(ctx.read_df("passing_grades"), ["student_id"]) \
    .to_wf_spec()

utils.info("  student_grades:     {} op(s)".format(len(student_grades_bp.jobs_operations)))

# --- WF 37: course_enrollment <- passing_grades x course_id_name [on course_id] ---
# OUTPUT COLS: student_id, course_id, grade, semester, course_name
course_enrollment_bp = ctx.read_df("passing_grades") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  course_enrollment:  {} op(s)".format(len(course_enrollment_bp.jobs_operations)))

# --- WF 38: faculty_dept <- tenured_faculty x dept_lookup [on dept_code] ---
# OUTPUT COLS: faculty_id, faculty_name, dept_code, faculty_title, dept_name
faculty_dept_bp = ctx.read_df("tenured_faculty") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  faculty_dept:       {} op(s)".format(len(faculty_dept_bp.jobs_operations)))

# --- WF 39: student_dept <- active_students x dept_lookup [on dept_code] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, dept_name
student_dept_bp = ctx.read_df("active_students") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  student_dept:       {} op(s)".format(len(student_dept_bp.jobs_operations)))

# --- WF 40: full_transcript <- student_grades x courses [on course_id, rsuffix=crs] ---
# LEFT:  student_grades -> student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester
# RIGHT: courses        -> course_id, course_name, dept_code, credits, level
# Shared non-key: dept_code -> becomes dept_code:crs
# ALL right non-key cols get :crs suffix: course_name:crs, dept_code:crs, credits:crs, level:crs
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester, course_name:crs, dept_code:crs, credits:crs, level:crs
full_transcript_bp = ctx.read_df("student_grades") \
    .inner_map_join(ctx.read_df("courses"), ["course_id"], rsuffix = "crs") \
    .to_wf_spec()

utils.info("  full_transcript:    {} op(s)".format(len(full_transcript_bp.jobs_operations)))

# --- WF 41: schedule_room <- fall_schedules x large_rooms [on room_id] ---
# OUTPUT COLS: schedule_id, course_id, room_id, faculty_id, time_slot, semester, building, capacity, room_type
schedule_room_bp = ctx.read_df("fall_schedules") \
    .inner_map_join(ctx.read_df("large_rooms"), ["room_id"]) \
    .to_wf_spec()

utils.info("  schedule_room:      {} op(s)".format(len(schedule_room_bp.jobs_operations)))

# --- WF 42: schedule_course <- fall_schedules x course_id_name [on course_id] ---
# OUTPUT COLS: schedule_id, course_id, room_id, faculty_id, time_slot, semester, course_name
schedule_course_bp = ctx.read_df("fall_schedules") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  schedule_course:    {} op(s)".format(len(schedule_course_bp.jobs_operations)))

# --- WF 43: prereq_names <- prerequisites x course_id_name [on course_id] ---
# OUTPUT COLS: course_id, prereq_id, course_name
prereq_names_bp = ctx.read_df("prerequisites") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  prereq_names:       {} op(s)".format(len(prereq_names_bp.jobs_operations)))

# --- WF 44: faculty_grants <- tenured_faculty x large_grants [on faculty_id] ---
# OUTPUT COLS: faculty_id, faculty_name, dept_code, faculty_title, grant_id, grant_amount, grant_title, grant_year
faculty_grants_bp = ctx.read_df("tenured_faculty") \
    .inner_map_join(ctx.read_df("large_grants"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  faculty_grants:     {} op(s)".format(len(faculty_grants_bp.jobs_operations)))

# --- WF 45: faculty_pubs <- faculty_id_name x recent_pubs [on faculty_id] ---
# OUTPUT COLS: faculty_id, faculty_name, pub_id, pub_title, venue, pub_year
faculty_pubs_bp = ctx.read_df("faculty_id_name") \
    .inner_map_join(ctx.read_df("recent_pubs"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  faculty_pubs:       {} op(s)".format(len(faculty_pubs_bp.jobs_operations)))

# --- WF 46: member_club <- club_members x club_lookup [on club_id] ---
# OUTPUT COLS: student_id, club_id, member_role, club_name
member_club_bp = ctx.read_df("club_members") \
    .inner_map_join(ctx.read_df("club_lookup"), ["club_id"]) \
    .to_wf_spec()

utils.info("  member_club:        {} op(s)".format(len(member_club_bp.jobs_operations)))

# --- WF 47: member_student <- club_members x student_id_name [on student_id] ---
# OUTPUT COLS: student_id, club_id, member_role, student_name
member_student_bp = ctx.read_df("club_members") \
    .inner_map_join(ctx.read_df("student_id_name"), ["student_id"]) \
    .to_wf_spec()

utils.info("  member_student:     {} op(s)".format(len(member_student_bp.jobs_operations)))

# --- WF 48: high_gpa_dept <- high_gpa_students x dept_lookup [on dept_code] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, dept_name
high_gpa_dept_bp = ctx.read_df("high_gpa_students") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  high_gpa_dept:      {} op(s)".format(len(high_gpa_dept_bp.jobs_operations)))

# --- WF 49: senior_dept <- senior_students x dept_lookup [on dept_code] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, dept_name
senior_dept_bp = ctx.read_df("senior_students") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  senior_dept:        {} op(s)".format(len(senior_dept_bp.jobs_operations)))

# --- WF 50: club_dept <- clubs x dept_lookup [on dept_code] ---
# OUTPUT COLS: club_id, club_name, dept_code, club_budget, dept_name
club_dept_bp = ctx.read_df("clubs") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  club_dept:          {} op(s)".format(len(club_dept_bp.jobs_operations)))

# --- WF 51: book_dept <- library_books x dept_lookup [on dept_code] ---
# OUTPUT COLS: book_id, book_title, book_author, dept_code, copies_total, copies_available, dept_name
book_dept_bp = ctx.read_df("library_books") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  book_dept:          {} op(s)".format(len(book_dept_bp.jobs_operations)))

# --- WF 52: advisor_faculty <- research_advisors x faculty_id_name [on faculty_id] ---
# OUTPUT COLS: advisor_id, faculty_id, student_id, advisor_type, start_year, faculty_name
advisor_faculty_bp = ctx.read_df("research_advisors") \
    .inner_map_join(ctx.read_df("faculty_id_name"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  advisor_faculty:    {} op(s)".format(len(advisor_faculty_bp.jobs_operations)))

# --- WF 53: advisor_student <- research_advisors x student_id_name [on student_id] ---
# OUTPUT COLS: advisor_id, faculty_id, student_id, advisor_type, start_year, student_name
advisor_student_bp = ctx.read_df("research_advisors") \
    .inner_map_join(ctx.read_df("student_id_name"), ["student_id"]) \
    .to_wf_spec()

utils.info("  advisor_student:    {} op(s)".format(len(advisor_student_bp.jobs_operations)))

# --- WF 54: intern_student <- high_stipend_interns x student_id_name [on student_id] ---
# OUTPUT COLS: internship_id, student_id, company_name, intern_role, intern_semester, intern_stipend, student_name
intern_student_bp = ctx.read_df("high_stipend_interns") \
    .inner_map_join(ctx.read_df("student_id_name"), ["student_id"]) \
    .to_wf_spec()

utils.info("  intern_student:     {} op(s)".format(len(intern_student_bp.jobs_operations)))

# --- WF 55: cs_enrollment <- cs_students x passing_grades [on student_id] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester
cs_enrollment_bp = ctx.read_df("cs_students") \
    .inner_map_join(ctx.read_df("passing_grades"), ["student_id"]) \
    .to_wf_spec()

utils.info("  cs_enrollment:      {} op(s)".format(len(cs_enrollment_bp.jobs_operations)))

# --- WF 56: upper_course_sched <- upper_courses x fall_schedules [on course_id] ---
# OUTPUT COLS: course_id, course_name, dept_code, credits, level, schedule_id, room_id, faculty_id, time_slot, semester
upper_course_sched_bp = ctx.read_df("upper_courses") \
    .inner_map_join(ctx.read_df("fall_schedules"), ["course_id"]) \
    .to_wf_spec()

utils.info("  upper_course_sched: {} op(s)".format(len(upper_course_sched_bp.jobs_operations)))

# --- WF 57: faculty_dept_select <- faculty_dept [select faculty_id, dept_code, dept_name] ---
# OUTPUT COLS: faculty_id, dept_code, dept_name
faculty_dept_select_bp = ctx.read_df("faculty_dept") \
    .select(["faculty_id", "dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  faculty_dept_select: {} op(s)".format(len(faculty_dept_select_bp.jobs_operations)))

# --- WF 58: enrollment_slim <- passing_grades [select student_id, course_id, grade] ---
# OUTPUT COLS: student_id, course_id, grade
enrollment_slim_bp = ctx.read_df("passing_grades") \
    .select(["student_id", "course_id", "grade"]) \
    .to_wf_spec()

utils.info("  enrollment_slim:    {} op(s)".format(len(enrollment_slim_bp.jobs_operations)))

# --- WF 59: grant_faculty_dept <- large_grants x faculty x dept_lookup [composite join] ---
# OUTPUT COLS: grant_id, faculty_id, grant_amount, grant_title, grant_year, faculty_name, dept_code, dept_name
grant_faculty_dept_bp = ctx.read_df("large_grants") \
    .inner_map_join(ctx.read_df("faculty"), ["faculty_id"]) \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  grant_faculty_dept: {} op(s)".format(len(grant_faculty_dept_bp.jobs_operations)))

# --- WF 60: schedule_faculty <- fall_schedules x faculty_id_name [on faculty_id] ---
# OUTPUT COLS: schedule_id, course_id, room_id, faculty_id, time_slot, semester, faculty_name
schedule_faculty_bp = ctx.read_df("fall_schedules") \
    .inner_map_join(ctx.read_df("faculty_id_name"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  schedule_faculty:   {} op(s)".format(len(schedule_faculty_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 3 — Secondary enrichment (25 WFs): WFs 61-85
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 3: Secondary enrichment (25 WFs) ---")

# --- WF 61: high_gpa_enrollment <- high_gpa_students x enrollment_slim [on student_id] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade
high_gpa_enrollment_bp = ctx.read_df("high_gpa_students") \
    .inner_map_join(ctx.read_df("enrollment_slim"), ["student_id"]) \
    .to_wf_spec()

utils.info("  high_gpa_enrollment: {} op(s)".format(len(high_gpa_enrollment_bp.jobs_operations)))

# --- WF 62: senior_enrollment <- senior_students x enrollment_slim [on student_id] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade
senior_enrollment_bp = ctx.read_df("senior_students") \
    .inner_map_join(ctx.read_df("enrollment_slim"), ["student_id"]) \
    .to_wf_spec()

utils.info("  senior_enrollment:  {} op(s)".format(len(senior_enrollment_bp.jobs_operations)))

# --- WF 63: upper_enrollment <- upper_courses x enrollment_slim [on course_id] ---
# OUTPUT COLS: course_id, course_name, dept_code, credits, level, student_id, grade
upper_enrollment_bp = ctx.read_df("upper_courses") \
    .inner_map_join(ctx.read_df("enrollment_slim"), ["course_id"]) \
    .to_wf_spec()

utils.info("  upper_enrollment:   {} op(s)".format(len(upper_enrollment_bp.jobs_operations)))

# --- WF 64: grant_dept <- faculty_grants x dept_lookup [on dept_code] ---
# OUTPUT COLS: faculty_id, faculty_name, dept_code, faculty_title, grant_id, grant_amount, grant_title, grant_year, dept_name
grant_dept_bp = ctx.read_df("faculty_grants") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  grant_dept:         {} op(s)".format(len(grant_dept_bp.jobs_operations)))

# --- WF 65: pub_dept <- faculty_pubs x faculty_dept_select [on faculty_id] ---
# OUTPUT COLS: faculty_id, faculty_name, pub_id, pub_title, venue, pub_year, dept_code, dept_name
pub_dept_bp = ctx.read_df("faculty_pubs") \
    .inner_map_join(ctx.read_df("faculty_dept_select"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  pub_dept:           {} op(s)".format(len(pub_dept_bp.jobs_operations)))

# --- WF 66: member_club_student <- member_club x student_id_name [on student_id] ---
# OUTPUT COLS: student_id, club_id, member_role, club_name, student_name
member_club_student_bp = ctx.read_df("member_club") \
    .inner_map_join(ctx.read_df("student_id_name"), ["student_id"]) \
    .to_wf_spec()

utils.info("  member_club_student: {} op(s)".format(len(member_club_student_bp.jobs_operations)))

# --- WF 67: high_gpa_course_enrich <- high_gpa_enrollment x course_id_name [on course_id] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade, course_name
high_gpa_course_enrich_bp = ctx.read_df("high_gpa_enrollment") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  high_gpa_course_enrich: {} op(s)".format(len(high_gpa_course_enrich_bp.jobs_operations)))

# --- WF 68: senior_course_enrich <- senior_enrollment x course_id_name [on course_id] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade, course_name
senior_course_enrich_bp = ctx.read_df("senior_enrollment") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  senior_course_enrich: {} op(s)".format(len(senior_course_enrich_bp.jobs_operations)))

# --- WF 69: upper_student_enrich <- upper_enrollment x student_id_name [on student_id] ---
# OUTPUT COLS: course_id, course_name, dept_code, credits, level, student_id, grade, student_name
upper_student_enrich_bp = ctx.read_df("upper_enrollment") \
    .inner_map_join(ctx.read_df("student_id_name"), ["student_id"]) \
    .to_wf_spec()

utils.info("  upper_student_enrich: {} op(s)".format(len(upper_student_enrich_bp.jobs_operations)))

# --- WF 70: club_dept_budget <- club_dept x dept_budget_lookup [on dept_code] ---
# OUTPUT COLS: club_id, club_name, dept_code, club_budget, dept_name, budget
club_dept_budget_bp = ctx.read_df("club_dept") \
    .inner_map_join(ctx.read_df("dept_budget_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  club_dept_budget:   {} op(s)".format(len(club_dept_budget_bp.jobs_operations)))

# --- WF 71: schedule_full <- schedule_course x large_rooms [on room_id] ---
# OUTPUT COLS: schedule_id, course_id, room_id, faculty_id, time_slot, semester, course_name, building, capacity, room_type
schedule_full_bp = ctx.read_df("schedule_course") \
    .inner_map_join(ctx.read_df("large_rooms"), ["room_id"]) \
    .to_wf_spec()

utils.info("  schedule_full:      {} op(s)".format(len(schedule_full_bp.jobs_operations)))

# --- WF 72: advisor_dept <- advisor_faculty x faculty_dept_select [on faculty_id] ---
# OUTPUT COLS: advisor_id, faculty_id, student_id, advisor_type, start_year, faculty_name, dept_code, dept_name
advisor_dept_bp = ctx.read_df("advisor_faculty") \
    .inner_map_join(ctx.read_df("faculty_dept_select"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  advisor_dept:       {} op(s)".format(len(advisor_dept_bp.jobs_operations)))

# --- WF 73: intern_dept <- intern_student x student_dept [on student_id, rsuffix=id] ---
# LEFT:  intern_student -> internship_id, student_id, company_name, intern_role, intern_semester, intern_stipend, student_name
# RIGHT: student_dept   -> student_id, student_name, gpa, dept_code, year, status, dept_name
# ALL right non-key cols get :id suffix: student_name:id, gpa:id, dept_code:id, year:id, status:id, dept_name:id
# After drop student_name:id and rename suffixed cols back to usable names
# OUTPUT COLS: internship_id, student_id, company_name, intern_role, intern_semester, intern_stipend, student_name, gpa, dept_code, year, status, dept_name
intern_dept_bp = ctx.read_df("intern_student") \
    .inner_map_join(ctx.read_df("student_dept"), ["student_id"], rsuffix = "id") \
    .drop_cols(["student_name:id"]) \
    .rename("gpa:id", "gpa") \
    .rename("dept_code:id", "dept_code") \
    .rename("year:id", "year") \
    .rename("status:id", "status") \
    .rename("dept_name:id", "dept_name") \
    .to_wf_spec()

utils.info("  intern_dept:        {} op(s)".format(len(intern_dept_bp.jobs_operations)))

# --- WF 74: intern_grade <- intern_student x enrollment_slim [on student_id] ---
# OUTPUT COLS: internship_id, student_id, company_name, intern_role, intern_semester, intern_stipend, student_name, course_id, grade
intern_grade_bp = ctx.read_df("intern_student") \
    .inner_map_join(ctx.read_df("enrollment_slim"), ["student_id"]) \
    .to_wf_spec()

utils.info("  intern_grade:       {} op(s)".format(len(intern_grade_bp.jobs_operations)))

# --- WF 75: prereq_count <- prereq_names [group_count by course_id, course_name] ---
# OUTPUT COLS: course_id, course_name, group:count
prereq_count_bp = ctx.read_df("prereq_names") \
    .group_count(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  prereq_count:       {} op(s)".format(len(prereq_count_bp.jobs_operations)))

# --- WF 76: club_member_count <- member_club [group_count by club_id, club_name] ---
# OUTPUT COLS: club_id, club_name, group:count
club_member_count_bp = ctx.read_df("member_club") \
    .group_count(["club_id", "club_name"]) \
    .to_wf_spec()

utils.info("  club_member_count:  {} op(s)".format(len(club_member_count_bp.jobs_operations)))

# --- WF 77: student_club_count <- member_club [group_count by student_id] ---
# OUTPUT COLS: student_id, group:count
student_club_count_bp = ctx.read_df("member_club") \
    .group_count(["student_id"]) \
    .to_wf_spec()

utils.info("  student_club_count: {} op(s)".format(len(student_club_count_bp.jobs_operations)))

# --- WF 78: book_dept_budget <- book_dept x dept_budget_lookup [on dept_code] ---
# OUTPUT COLS: book_id, book_title, book_author, dept_code, copies_total, copies_available, dept_name, budget
book_dept_budget_bp = ctx.read_df("book_dept") \
    .inner_map_join(ctx.read_df("dept_budget_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  book_dept_budget:   {} op(s)".format(len(book_dept_budget_bp.jobs_operations)))

# --- WF 79: cs_course_enroll <- cs_enrollment x course_id_name [on course_id] ---
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester, course_name
cs_course_enroll_bp = ctx.read_df("cs_enrollment") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  cs_course_enroll:   {} op(s)".format(len(cs_course_enroll_bp.jobs_operations)))

# --- WF 80: schedule_room_faculty <- schedule_room x faculty_id_name [on faculty_id] ---
# OUTPUT COLS: schedule_id, course_id, room_id, faculty_id, time_slot, semester, building, capacity, room_type, faculty_name
schedule_room_faculty_bp = ctx.read_df("schedule_room") \
    .inner_map_join(ctx.read_df("faculty_id_name"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  schedule_room_faculty: {} op(s)".format(len(schedule_room_faculty_bp.jobs_operations)))

# --- WF 81: book_count <- library_books [group_count by dept_code] ---
# OUTPUT COLS: dept_code, group:count
book_count_bp = ctx.read_df("library_books") \
    .group_count(["dept_code"]) \
    .to_wf_spec()

utils.info("  book_count:         {} op(s)".format(len(book_count_bp.jobs_operations)))

# --- WF 82: advisor_count <- advisors [group_count by faculty_id] ---
# OUTPUT COLS: faculty_id, group:count
advisor_count_bp = ctx.read_df("advisors") \
    .group_count(["faculty_id"]) \
    .to_wf_spec()

utils.info("  advisor_count:      {} op(s)".format(len(advisor_count_bp.jobs_operations)))

# --- WF 83: intern_count <- internships [group_count by student_id] ---
# OUTPUT COLS: student_id, group:count
intern_count_bp = ctx.read_df("internships") \
    .group_count(["student_id"]) \
    .to_wf_spec()

utils.info("  intern_count:       {} op(s)".format(len(intern_count_bp.jobs_operations)))

# --- WF 84: transcript_slim <- full_transcript [select student_id, course_id, grade, course_name:crs] ---
# INPUT: full_transcript -> student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester, course_name:crs, dept_code:crs, credits:crs, level:crs
# OUTPUT COLS: student_id, course_id, grade, course_name:crs
transcript_slim_bp = ctx.read_df("full_transcript") \
    .select(["student_id", "course_id", "grade", "course_name:crs"]) \
    .to_wf_spec()

utils.info("  transcript_slim:    {} op(s)".format(len(transcript_slim_bp.jobs_operations)))

# --- WF 85: student_grade_select <- student_grades [select student_id, student_name, course_id, grade] ---
# OUTPUT COLS: student_id, student_name, course_id, grade
student_grade_select_bp = ctx.read_df("student_grades") \
    .select(["student_id", "student_name", "course_id", "grade"]) \
    .to_wf_spec()

utils.info("  student_grade_select: {} op(s)".format(len(student_grade_select_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 4 — Aggregations (25 WFs): WFs 86-110
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 4: Aggregations (25 WFs) ---")

# --- WF 86: student_load <- enrollment_slim [group_count by student_id] ---
# OUTPUT COLS: student_id, group:count
student_load_bp = ctx.read_df("enrollment_slim") \
    .group_count(["student_id"]) \
    .to_wf_spec()

utils.info("  student_load:       {} op(s)".format(len(student_load_bp.jobs_operations)))

# --- WF 87: course_demand <- enrollment_slim [group_count by course_id] ---
# OUTPUT COLS: course_id, group:count
course_demand_bp = ctx.read_df("enrollment_slim") \
    .group_count(["course_id"]) \
    .to_wf_spec()

utils.info("  course_demand:      {} op(s)".format(len(course_demand_bp.jobs_operations)))

# --- WF 88: dept_enrollment <- student_dept [group_count by dept_code, dept_name] ---
# OUTPUT COLS: dept_code, dept_name, group:count
dept_enrollment_bp = ctx.read_df("student_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  dept_enrollment:    {} op(s)".format(len(dept_enrollment_bp.jobs_operations)))

# --- WF 89: grant_by_dept <- grant_dept [group_count by dept_code, dept_name] ---
# OUTPUT COLS: dept_code, dept_name, group:count
grant_by_dept_bp = ctx.read_df("grant_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  grant_by_dept:      {} op(s)".format(len(grant_by_dept_bp.jobs_operations)))

# --- WF 90: pub_by_dept <- pub_dept [group_count by dept_code, dept_name] ---
# OUTPUT COLS: dept_code, dept_name, group:count
pub_by_dept_bp = ctx.read_df("pub_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  pub_by_dept:        {} op(s)".format(len(pub_by_dept_bp.jobs_operations)))

# --- WF 91: pub_by_faculty <- faculty_pubs [group_count by faculty_id, faculty_name] ---
# OUTPUT COLS: faculty_id, faculty_name, group:count
pub_by_faculty_bp = ctx.read_df("faculty_pubs") \
    .group_count(["faculty_id", "faculty_name"]) \
    .to_wf_spec()

utils.info("  pub_by_faculty:     {} op(s)".format(len(pub_by_faculty_bp.jobs_operations)))

# --- WF 92: grant_by_faculty <- faculty_grants [group_count by faculty_id, faculty_name] ---
# OUTPUT COLS: faculty_id, faculty_name, group:count
grant_by_faculty_bp = ctx.read_df("faculty_grants") \
    .group_count(["faculty_id", "faculty_name"]) \
    .to_wf_spec()

utils.info("  grant_by_faculty:   {} op(s)".format(len(grant_by_faculty_bp.jobs_operations)))

# --- WF 93: high_gpa_by_dept <- high_gpa_dept [group_count by dept_code, dept_name] ---
# OUTPUT COLS: dept_code, dept_name, group:count
high_gpa_by_dept_bp = ctx.read_df("high_gpa_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  high_gpa_by_dept:   {} op(s)".format(len(high_gpa_by_dept_bp.jobs_operations)))

# --- WF 94: senior_by_dept <- senior_dept [group_count by dept_code, dept_name] ---
# OUTPUT COLS: dept_code, dept_name, group:count
senior_by_dept_bp = ctx.read_df("senior_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  senior_by_dept:     {} op(s)".format(len(senior_by_dept_bp.jobs_operations)))

# --- WF 95: high_gpa_by_course <- high_gpa_course_enrich [group_count by course_id, course_name] ---
# OUTPUT COLS: course_id, course_name, group:count
high_gpa_by_course_bp = ctx.read_df("high_gpa_course_enrich") \
    .group_count(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  high_gpa_by_course: {} op(s)".format(len(high_gpa_by_course_bp.jobs_operations)))

# --- WF 96: senior_by_course <- senior_course_enrich [group_count by course_id, course_name] ---
# OUTPUT COLS: course_id, course_name, group:count
senior_by_course_bp = ctx.read_df("senior_course_enrich") \
    .group_count(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  senior_by_course:   {} op(s)".format(len(senior_by_course_bp.jobs_operations)))

# --- WF 97: upper_by_student <- upper_student_enrich [group_count by student_id, student_name] ---
# OUTPUT COLS: student_id, student_name, group:count
upper_by_student_bp = ctx.read_df("upper_student_enrich") \
    .group_count(["student_id", "student_name"]) \
    .to_wf_spec()

utils.info("  upper_by_student:   {} op(s)".format(len(upper_by_student_bp.jobs_operations)))

# --- WF 98: upper_by_dept <- upper_student_enrich [group_count by dept_code] ---
# OUTPUT COLS: dept_code, group:count
upper_by_dept_bp = ctx.read_df("upper_student_enrich") \
    .group_count(["dept_code"]) \
    .to_wf_spec()

utils.info("  upper_by_dept:      {} op(s)".format(len(upper_by_dept_bp.jobs_operations)))

# --- WF 99: schedule_by_building <- schedule_room [group_count by building] ---
# OUTPUT COLS: building, group:count
schedule_by_building_bp = ctx.read_df("schedule_room") \
    .group_count(["building"]) \
    .to_wf_spec()

utils.info("  schedule_by_building: {} op(s)".format(len(schedule_by_building_bp.jobs_operations)))

# --- WF 100: club_by_dept <- club_dept [group_count by dept_code, dept_name] ---
# OUTPUT COLS: dept_code, dept_name, group:count
club_by_dept_bp = ctx.read_df("club_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  club_by_dept:       {} op(s)".format(len(club_by_dept_bp.jobs_operations)))

# --- WF 101: advisor_by_dept <- advisor_dept [group_count by dept_code, dept_name] ---
# OUTPUT COLS: dept_code, dept_name, group:count
advisor_by_dept_bp = ctx.read_df("advisor_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  advisor_by_dept:    {} op(s)".format(len(advisor_by_dept_bp.jobs_operations)))

# --- WF 102: intern_by_company <- internships [group_count by company_name] ---
# OUTPUT COLS: company_name, group:count
intern_by_company_bp = ctx.read_df("internships") \
    .group_count(["company_name"]) \
    .to_wf_spec()

utils.info("  intern_by_company:  {} op(s)".format(len(intern_by_company_bp.jobs_operations)))

# --- WF 103: intern_by_dept <- intern_dept [group_count by dept_code] ---
# INPUT: intern_dept -> internship_id, student_id, company_name, intern_role, intern_semester, intern_stipend, student_name, gpa, dept_code, year, status, dept_name
# OUTPUT COLS: dept_code, group:count
intern_by_dept_bp = ctx.read_df("intern_dept") \
    .group_count(["dept_code"]) \
    .to_wf_spec()

utils.info("  intern_by_dept:     {} op(s)".format(len(intern_by_dept_bp.jobs_operations)))

# --- WF 104: book_by_dept <- library_books [group_count by dept_code] ---
# OUTPUT COLS: dept_code, group:count
book_by_dept_bp = ctx.read_df("library_books") \
    .group_count(["dept_code"]) \
    .to_wf_spec()

utils.info("  book_by_dept:       {} op(s)".format(len(book_by_dept_bp.jobs_operations)))

# --- WF 105: advisor_by_student <- advisors [group_count by student_id] ---
# OUTPUT COLS: student_id, group:count
advisor_by_student_bp = ctx.read_df("advisors") \
    .group_count(["student_id"]) \
    .to_wf_spec()

utils.info("  advisor_by_student: {} op(s)".format(len(advisor_by_student_bp.jobs_operations)))

# --- WF 106: grade_distribution <- enrollments [group_count by grade] ---
# OUTPUT COLS: grade, group:count
grade_distribution_bp = ctx.read_df("enrollments") \
    .group_count(["grade"]) \
    .to_wf_spec()

utils.info("  grade_distribution: {} op(s)".format(len(grade_distribution_bp.jobs_operations)))

# --- WF 107: course_by_level <- courses [group_count by level] ---
# OUTPUT COLS: level, group:count
course_by_level_bp = ctx.read_df("courses") \
    .group_count(["level"]) \
    .to_wf_spec()

utils.info("  course_by_level:    {} op(s)".format(len(course_by_level_bp.jobs_operations)))

# --- WF 108: student_by_year <- students [group_count by year] ---
# OUTPUT COLS: year, group:count
student_by_year_bp = ctx.read_df("students") \
    .group_count(["year"]) \
    .to_wf_spec()

utils.info("  student_by_year:    {} op(s)".format(len(student_by_year_bp.jobs_operations)))

# --- WF 109: cs_by_course <- cs_course_enroll [group_count by course_id, course_name] ---
# OUTPUT COLS: course_id, course_name, group:count
cs_by_course_bp = ctx.read_df("cs_course_enroll") \
    .group_count(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  cs_by_course:       {} op(s)".format(len(cs_by_course_bp.jobs_operations)))

# --- WF 110: faculty_by_dept <- faculty_dept [group_count by dept_code, dept_name] ---
# OUTPUT COLS: dept_code, dept_name, group:count
faculty_by_dept_bp = ctx.read_df("faculty_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  faculty_by_dept:    {} op(s)".format(len(faculty_by_dept_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 5 — Tertiary analytics (25 WFs): WFs 111-135
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 5: Tertiary analytics (25 WFs) ---")

# --- WF 111: student_load_name <- student_load x student_id_name [on student_id] ---
student_load_name_bp = ctx.read_df("student_load") \
    .inner_map_join(ctx.read_df("student_id_name"), ["student_id"]) \
    .to_wf_spec()

utils.info("  student_load_name:  {} op(s)".format(len(student_load_name_bp.jobs_operations)))

# --- WF 112: course_demand_name <- course_demand x course_id_name [on course_id] ---
course_demand_name_bp = ctx.read_df("course_demand") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  course_demand_name: {} op(s)".format(len(course_demand_name_bp.jobs_operations)))

# --- WF 113: pub_faculty_name <- pub_by_faculty [already has faculty_name] ---
pub_faculty_name_bp = ctx.read_df("pub_by_faculty") \
    .select(["faculty_id", "faculty_name", "group:count"]) \
    .to_wf_spec()

utils.info("  pub_faculty_name:   {} op(s)".format(len(pub_faculty_name_bp.jobs_operations)))

# --- WF 114: grant_faculty_name <- grant_by_faculty [already has faculty_name] ---
grant_faculty_name_bp = ctx.read_df("grant_by_faculty") \
    .select(["faculty_id", "faculty_name", "group:count"]) \
    .to_wf_spec()

utils.info("  grant_faculty_name: {} op(s)".format(len(grant_faculty_name_bp.jobs_operations)))

# --- WF 115: advisor_student_name <- advisor_by_student x student_id_name [on student_id] ---
advisor_student_name_bp = ctx.read_df("advisor_by_student") \
    .inner_map_join(ctx.read_df("student_id_name"), ["student_id"]) \
    .to_wf_spec()

utils.info("  advisor_student_name: {} op(s)".format(len(advisor_student_name_bp.jobs_operations)))

# --- WF 116: student_load_sorted <- student_load_name [sort by group:count desc, add rank] ---
student_load_sorted_bp = ctx.read_df("student_load_name") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  student_load_sorted: {} op(s)".format(len(student_load_sorted_bp.jobs_operations)))

# --- WF 117: course_demand_sorted <- course_demand_name [sort by group:count desc, add rank] ---
course_demand_sorted_bp = ctx.read_df("course_demand_name") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  course_demand_sorted: {} op(s)".format(len(course_demand_sorted_bp.jobs_operations)))

# --- WF 118: high_load_students <- student_load_name [filter group:count >= 2] ---
high_load_students_bp = ctx.read_df("student_load_name") \
    .ge_int("group:count", 2) \
    .to_wf_spec()

utils.info("  high_load_students: {} op(s)".format(len(high_load_students_bp.jobs_operations)))

# --- WF 119: popular_courses <- course_demand_name [filter group:count >= 3] ---
popular_courses_bp = ctx.read_df("course_demand_name") \
    .ge_int("group:count", 3) \
    .to_wf_spec()

utils.info("  popular_courses:    {} op(s)".format(len(popular_courses_bp.jobs_operations)))

# --- WF 120: active_advisors <- advisor_by_dept [filter group:count >= 2] ---
active_advisors_bp = ctx.read_df("advisor_by_dept") \
    .ge_int("group:count", 2) \
    .to_wf_spec()

utils.info("  active_advisors:    {} op(s)".format(len(active_advisors_bp.jobs_operations)))

# --- WF 121: grade_dist_sorted <- grade_distribution [sort by group:count desc, add rank] ---
grade_dist_sorted_bp = ctx.read_df("grade_distribution") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  grade_dist_sorted:  {} op(s)".format(len(grade_dist_sorted_bp.jobs_operations)))

# --- WF 122: intern_dept_name <- intern_by_dept x dept_lookup [on dept_code] ---
intern_dept_name_bp = ctx.read_df("intern_by_dept") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  intern_dept_name:   {} op(s)".format(len(intern_dept_name_bp.jobs_operations)))

# --- WF 123: book_dept_name <- book_by_dept x dept_lookup [on dept_code] ---
book_dept_name_bp = ctx.read_df("book_by_dept") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  book_dept_name:     {} op(s)".format(len(book_dept_name_bp.jobs_operations)))

# --- WF 124: student_year_label <- student_by_year [pass through] ---
student_year_label_bp = ctx.read_df("student_by_year") \
    .select(["year", "group:count"]) \
    .to_wf_spec()

utils.info("  student_year_label: {} op(s)".format(len(student_year_label_bp.jobs_operations)))

# --- WF 125: course_level_label <- course_by_level [pass through] ---
course_level_label_bp = ctx.read_df("course_by_level") \
    .select(["level", "group:count"]) \
    .to_wf_spec()

utils.info("  course_level_label: {} op(s)".format(len(course_level_label_bp.jobs_operations)))

# --- WF 126: intern_company_count <- intern_by_company [select] ---
intern_company_count_bp = ctx.read_df("intern_by_company") \
    .select(["company_name", "group:count"]) \
    .to_wf_spec()

utils.info("  intern_company_count: {} op(s)".format(len(intern_company_count_bp.jobs_operations)))

# --- WF 127: upper_dept_name <- upper_by_dept x dept_lookup [on dept_code] ---
upper_dept_name_bp = ctx.read_df("upper_by_dept") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  upper_dept_name:    {} op(s)".format(len(upper_dept_name_bp.jobs_operations)))

# --- WF 128: grant_dept_sorted <- grant_by_dept [sort, add rank] ---
grant_dept_sorted_bp = ctx.read_df("grant_by_dept") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  grant_dept_sorted:  {} op(s)".format(len(grant_dept_sorted_bp.jobs_operations)))

# --- WF 129: pub_dept_sorted <- pub_by_dept [sort, add rank] ---
pub_dept_sorted_bp = ctx.read_df("pub_by_dept") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  pub_dept_sorted:    {} op(s)".format(len(pub_dept_sorted_bp.jobs_operations)))

# --- WF 130: high_gpa_dept_sorted <- high_gpa_by_dept [sort, add rank] ---
high_gpa_dept_sorted_bp = ctx.read_df("high_gpa_by_dept") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  high_gpa_dept_sorted: {} op(s)".format(len(high_gpa_dept_sorted_bp.jobs_operations)))

# --- WF 131: senior_dept_sorted <- senior_by_dept [sort, add rank] ---
senior_dept_sorted_bp = ctx.read_df("senior_by_dept") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  senior_dept_sorted: {} op(s)".format(len(senior_dept_sorted_bp.jobs_operations)))

# --- WF 132: club_dept_sorted <- club_by_dept [sort, add rank] ---
club_dept_sorted_bp = ctx.read_df("club_by_dept") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  club_dept_sorted:   {} op(s)".format(len(club_dept_sorted_bp.jobs_operations)))

# --- WF 133: cs_course_sorted <- cs_by_course [sort, add rank] ---
cs_course_sorted_bp = ctx.read_df("cs_by_course") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  cs_course_sorted:   {} op(s)".format(len(cs_course_sorted_bp.jobs_operations)))

# --- WF 134: high_gpa_course_sorted <- high_gpa_by_course [sort, add rank] ---
high_gpa_course_sorted_bp = ctx.read_df("high_gpa_by_course") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  high_gpa_course_sorted: {} op(s)".format(len(high_gpa_course_sorted_bp.jobs_operations)))

# --- WF 135: senior_course_sorted <- senior_by_course [sort, add rank] ---
senior_course_sorted_bp = ctx.read_df("senior_by_course") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  senior_course_sorted: {} op(s)".format(len(senior_course_sorted_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 6 — Cross-domain analytics (25 WFs): WFs 136-160
# Joining Phase 5 aggregation outputs across different domains
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 6: Cross-domain analytics (25 WFs) ---")

# --- WF 136: dept_grant_pub <- grant_dept_sorted x pub_dept_sorted [on dept_code, dept_name] ---
dept_grant_pub_bp = ctx.read_df("grant_dept_sorted") \
    .inner_map_join(ctx.read_df("pub_dept_sorted"), ["dept_code", "dept_name"], rsuffix = "pub") \
    .to_wf_spec()

utils.info("  dept_grant_pub:     {} op(s)".format(len(dept_grant_pub_bp.jobs_operations)))

# --- WF 137: dept_students_clubs <- high_gpa_dept_sorted x club_dept_sorted [on dept_code, dept_name] ---
dept_students_clubs_bp = ctx.read_df("high_gpa_dept_sorted") \
    .inner_map_join(ctx.read_df("club_dept_sorted"), ["dept_code", "dept_name"], rsuffix = "club") \
    .to_wf_spec()

utils.info("  dept_students_clubs: {} op(s)".format(len(dept_students_clubs_bp.jobs_operations)))

# --- WF 138: student_intern_club <- high_load_students x student_club_count [on student_id] ---
student_intern_club_bp = ctx.read_df("high_load_students") \
    .inner_map_join(ctx.read_df("student_club_count"), ["student_id"], rsuffix = "club") \
    .to_wf_spec()

utils.info("  student_intern_club: {} op(s)".format(len(student_intern_club_bp.jobs_operations)))

# --- WF 139: course_demand_prereq <- popular_courses x prereq_count [on course_id] ---
course_demand_prereq_bp = ctx.read_df("popular_courses") \
    .inner_map_join(ctx.read_df("prereq_count"), ["course_id"], rsuffix = "prereq") \
    .to_wf_spec()

utils.info("  course_demand_prereq: {} op(s)".format(len(course_demand_prereq_bp.jobs_operations)))

# --- WF 140: faculty_grant_pub <- grant_faculty_name x pub_faculty_name [on faculty_id] ---
faculty_grant_pub_bp = ctx.read_df("grant_faculty_name") \
    .inner_map_join(ctx.read_df("pub_faculty_name"), ["faculty_id"], rsuffix = "pub") \
    .to_wf_spec()

utils.info("  faculty_grant_pub:  {} op(s)".format(len(faculty_grant_pub_bp.jobs_operations)))

# --- WF 141: dept_enrollment_books <- dept_enrollment x book_dept_name [on dept_code] ---
dept_enrollment_books_bp = ctx.read_df("dept_enrollment") \
    .inner_map_join(ctx.read_df("book_dept_name"), ["dept_code"], rsuffix = "book") \
    .to_wf_spec()

utils.info("  dept_enrollment_books: {} op(s)".format(len(dept_enrollment_books_bp.jobs_operations)))

# --- WF 142: dept_interns_advisors <- intern_dept_name x active_advisors [on dept_code] ---
dept_interns_advisors_bp = ctx.read_df("intern_dept_name") \
    .inner_map_join(ctx.read_df("active_advisors"), ["dept_code"], rsuffix = "adv") \
    .to_wf_spec()

utils.info("  dept_interns_advisors: {} op(s)".format(len(dept_interns_advisors_bp.jobs_operations)))

# --- WF 143: student_load_advisor <- student_load_sorted x advisor_student_name [on student_id] ---
student_load_advisor_bp = ctx.read_df("student_load_sorted") \
    .inner_map_join(ctx.read_df("advisor_student_name"), ["student_id"], rsuffix = "adv") \
    .to_wf_spec()

utils.info("  student_load_advisor: {} op(s)".format(len(student_load_advisor_bp.jobs_operations)))

# --- WF 144: cs_courses_demand <- cs_course_sorted x course_demand_sorted [on course_id] ---
cs_courses_demand_bp = ctx.read_df("cs_course_sorted") \
    .inner_map_join(ctx.read_df("course_demand_sorted"), ["course_id"], rsuffix = "all") \
    .to_wf_spec()

utils.info("  cs_courses_demand:  {} op(s)".format(len(cs_courses_demand_bp.jobs_operations)))

# --- WF 145: high_gpa_senior_courses <- high_gpa_course_sorted x senior_course_sorted [on course_id] ---
high_gpa_senior_courses_bp = ctx.read_df("high_gpa_course_sorted") \
    .inner_map_join(ctx.read_df("senior_course_sorted"), ["course_id"], rsuffix = "senior") \
    .to_wf_spec()

utils.info("  high_gpa_senior_courses: {} op(s)".format(len(high_gpa_senior_courses_bp.jobs_operations)))

# --- WF 146: dept_senior_high_gpa <- senior_dept_sorted x high_gpa_dept_sorted [on dept_code] ---
dept_senior_high_gpa_bp = ctx.read_df("senior_dept_sorted") \
    .inner_map_join(ctx.read_df("high_gpa_dept_sorted"), ["dept_code"], rsuffix = "hgpa") \
    .to_wf_spec()

utils.info("  dept_senior_high_gpa: {} op(s)".format(len(dept_senior_high_gpa_bp.jobs_operations)))

# --- WF 147: upper_dept_faculty <- upper_dept_name x faculty_by_dept [on dept_code] ---
upper_dept_faculty_bp = ctx.read_df("upper_dept_name") \
    .inner_map_join(ctx.read_df("faculty_by_dept"), ["dept_code"], rsuffix = "fac") \
    .to_wf_spec()

utils.info("  upper_dept_faculty: {} op(s)".format(len(upper_dept_faculty_bp.jobs_operations)))

# --- WF 148: schedule_building_demand <- schedule_by_building x dept_enrollment [via building+dept indirect] ---
# Simplified: just pass through schedule_by_building with select
schedule_building_summary_bp = ctx.read_df("schedule_by_building") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  schedule_building_summary: {} op(s)".format(len(schedule_building_summary_bp.jobs_operations)))

# --- WF 149: intern_company_dept <- intern_company_count x intern_dept_name [composite via student join] ---
# Simplified: select top companies
top_intern_companies_bp = ctx.read_df("intern_company_count") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  top_intern_companies: {} op(s)".format(len(top_intern_companies_bp.jobs_operations)))

# --- WF 150: year_level_distribution <- student_year_label x course_level_label [cartesian-like summary] ---
# Simplified: combine counts
combined_year_level_bp = ctx.read_df("student_year_label") \
    .sort(["year"], [True]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  combined_year_level: {} op(s)".format(len(combined_year_level_bp.jobs_operations)))

# --- WF 151-160: Additional cross-domain WFs ---
# These are simple derived analytics from Phase 5 outputs
# WF 151-160 will be filters, sorts, and aggregations on existing Phase 5 outputs

# --- WF 151: top_students <- student_load_sorted [filter rank <= 5] ---
top_students_bp = ctx.read_df("student_load_sorted") \
    .le_int("rank", 5) \
    .to_wf_spec()

utils.info("  top_students:       {} op(s)".format(len(top_students_bp.jobs_operations)))

# --- WF 152: top_courses <- course_demand_sorted [filter rank <= 5] ---
top_courses_bp = ctx.read_df("course_demand_sorted") \
    .le_int("rank", 5) \
    .to_wf_spec()

utils.info("  top_courses:        {} op(s)".format(len(top_courses_bp.jobs_operations)))

# --- WF 153: top_grant_depts <- grant_dept_sorted [filter rank <= 3] ---
top_grant_depts_bp = ctx.read_df("grant_dept_sorted") \
    .le_int("rank", 3) \
    .to_wf_spec()

utils.info("  top_grant_depts:    {} op(s)".format(len(top_grant_depts_bp.jobs_operations)))

# --- WF 154: top_pub_depts <- pub_dept_sorted [filter rank <= 3] ---
top_pub_depts_bp = ctx.read_df("pub_dept_sorted") \
    .le_int("rank", 3) \
    .to_wf_spec()

utils.info("  top_pub_depts:      {} op(s)".format(len(top_pub_depts_bp.jobs_operations)))

# --- WF 155: top_high_gpa_depts <- high_gpa_dept_sorted [filter rank <= 3] ---
top_high_gpa_depts_bp = ctx.read_df("high_gpa_dept_sorted") \
    .le_int("rank", 3) \
    .to_wf_spec()

utils.info("  top_high_gpa_depts: {} op(s)".format(len(top_high_gpa_depts_bp.jobs_operations)))

# --- WF 156: top_senior_depts <- senior_dept_sorted [filter rank <= 3] ---
top_senior_depts_bp = ctx.read_df("senior_dept_sorted") \
    .le_int("rank", 3) \
    .to_wf_spec()

utils.info("  top_senior_depts:   {} op(s)".format(len(top_senior_depts_bp.jobs_operations)))

# --- WF 157: top_club_depts <- club_dept_sorted [filter rank <= 3] ---
top_club_depts_bp = ctx.read_df("club_dept_sorted") \
    .le_int("rank", 3) \
    .to_wf_spec()

utils.info("  top_club_depts:     {} op(s)".format(len(top_club_depts_bp.jobs_operations)))

# --- WF 158: top_cs_courses <- cs_course_sorted [filter rank <= 5] ---
top_cs_courses_bp = ctx.read_df("cs_course_sorted") \
    .le_int("rank", 5) \
    .to_wf_spec()

utils.info("  top_cs_courses:     {} op(s)".format(len(top_cs_courses_bp.jobs_operations)))

# --- WF 159: top_high_gpa_courses <- high_gpa_course_sorted [filter rank <= 5] ---
top_high_gpa_courses_bp = ctx.read_df("high_gpa_course_sorted") \
    .le_int("rank", 5) \
    .to_wf_spec()

utils.info("  top_high_gpa_courses: {} op(s)".format(len(top_high_gpa_courses_bp.jobs_operations)))

# --- WF 160: top_senior_courses <- senior_course_sorted [filter rank <= 5] ---
top_senior_courses_bp = ctx.read_df("senior_course_sorted") \
    .le_int("rank", 5) \
    .to_wf_spec()

utils.info("  top_senior_courses: {} op(s)".format(len(top_senior_courses_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 7 — Comparative reports (15 WFs): WFs 161-175
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 7: Comparative reports (15 WFs) ---")

# --- WF 161: dept_research_compare <- dept_grant_pub [select key columns] ---
dept_research_compare_bp = ctx.read_df("dept_grant_pub") \
    .select(["dept_code", "dept_name", "group:count", "group:count:pub"]) \
    .to_wf_spec()

utils.info("  dept_research_compare: {} op(s)".format(len(dept_research_compare_bp.jobs_operations)))

# --- WF 162: dept_student_compare <- dept_students_clubs [select key columns] ---
dept_student_compare_bp = ctx.read_df("dept_students_clubs") \
    .select(["dept_code", "dept_name", "group:count", "group:count:club"]) \
    .to_wf_spec()

utils.info("  dept_student_compare: {} op(s)".format(len(dept_student_compare_bp.jobs_operations)))

# --- WF 163: course_popularity_compare <- high_gpa_senior_courses [select key columns] ---
course_popularity_compare_bp = ctx.read_df("high_gpa_senior_courses") \
    .select(["course_id", "course_name", "group:count", "group:count:senior"]) \
    .to_wf_spec()

utils.info("  course_popularity_compare: {} op(s)".format(len(course_popularity_compare_bp.jobs_operations)))

# --- WF 164: dept_performance_compare <- dept_senior_high_gpa [select key columns] ---
dept_performance_compare_bp = ctx.read_df("dept_senior_high_gpa") \
    .select(["dept_code", "dept_name", "group:count", "group:count:hgpa"]) \
    .to_wf_spec()

utils.info("  dept_performance_compare: {} op(s)".format(len(dept_performance_compare_bp.jobs_operations)))

# --- WF 165: student_engagement <- student_intern_club [select key columns] ---
student_engagement_bp = ctx.read_df("student_intern_club") \
    .select(["student_id", "student_name", "group:count", "group:count:club"]) \
    .to_wf_spec()

utils.info("  student_engagement: {} op(s)".format(len(student_engagement_bp.jobs_operations)))

# --- WF 166: faculty_productivity <- faculty_grant_pub [select key columns] ---
faculty_productivity_bp = ctx.read_df("faculty_grant_pub") \
    .select(["faculty_id", "faculty_name", "group:count", "group:count:pub"]) \
    .to_wf_spec()

utils.info("  faculty_productivity: {} op(s)".format(len(faculty_productivity_bp.jobs_operations)))

# --- WF 167: cs_demand_analysis <- cs_courses_demand [select key columns] ---
cs_demand_analysis_bp = ctx.read_df("cs_courses_demand") \
    .select(["course_id", "course_name", "group:count", "rank"]) \
    .to_wf_spec()

utils.info("  cs_demand_analysis: {} op(s)".format(len(cs_demand_analysis_bp.jobs_operations)))

# --- WF 168: student_advisor_load <- student_load_advisor [select key columns] ---
student_advisor_load_bp = ctx.read_df("student_load_advisor") \
    .select(["student_id", "student_name", "group:count", "rank"]) \
    .to_wf_spec()

utils.info("  student_advisor_load: {} op(s)".format(len(student_advisor_load_bp.jobs_operations)))

# --- WF 169: dept_resource_utilization <- dept_enrollment_books [select key columns] ---
dept_resource_util_bp = ctx.read_df("dept_enrollment_books") \
    .select(["dept_code", "dept_name", "group:count", "group:count:book"]) \
    .to_wf_spec()

utils.info("  dept_resource_util: {} op(s)".format(len(dept_resource_util_bp.jobs_operations)))

# --- WF 170: dept_intern_advisor_ratio <- dept_interns_advisors [select key columns] ---
dept_intern_advisor_ratio_bp = ctx.read_df("dept_interns_advisors") \
    .select(["dept_code", "dept_name", "group:count", "group:count:adv"]) \
    .to_wf_spec()

utils.info("  dept_intern_advisor_ratio: {} op(s)".format(len(dept_intern_advisor_ratio_bp.jobs_operations)))

# --- WF 171: upper_faculty_ratio <- upper_dept_faculty [select key columns] ---
upper_faculty_ratio_bp = ctx.read_df("upper_dept_faculty") \
    .select(["dept_code", "dept_name", "group:count", "group:count:fac"]) \
    .to_wf_spec()

utils.info("  upper_faculty_ratio: {} op(s)".format(len(upper_faculty_ratio_bp.jobs_operations)))

# --- WF 172: top_student_summary <- top_students [add metadata] ---
top_student_summary_bp = ctx.read_df("top_students") \
    .select(["student_id", "student_name", "group:count", "rank"]) \
    .to_wf_spec()

utils.info("  top_student_summary: {} op(s)".format(len(top_student_summary_bp.jobs_operations)))

# --- WF 173: top_course_summary <- top_courses [add metadata] ---
top_course_summary_bp = ctx.read_df("top_courses") \
    .select(["course_id", "course_name", "group:count", "rank"]) \
    .to_wf_spec()

utils.info("  top_course_summary: {} op(s)".format(len(top_course_summary_bp.jobs_operations)))

# --- WF 174: grade_distribution_analysis <- grade_dist_sorted [top grades] ---
grade_dist_analysis_bp = ctx.read_df("grade_dist_sorted") \
    .le_int("rank", 5) \
    .to_wf_spec()

utils.info("  grade_dist_analysis: {} op(s)".format(len(grade_dist_analysis_bp.jobs_operations)))

# --- WF 175: course_prereq_popularity <- course_demand_prereq [select key columns] ---
course_prereq_popularity_bp = ctx.read_df("course_demand_prereq") \
    .select(["course_id", "course_name", "group:count", "group:count:prereq"]) \
    .to_wf_spec()

utils.info("  course_prereq_popularity: {} op(s)".format(len(course_prereq_popularity_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 8 — Deep analytics (15 WFs): WFs 176-190
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 8: Deep analytics (15 WFs) ---")

# --- WF 176: top_dept_research_detail <- top_grant_depts x top_pub_depts [on dept_code] ---
top_dept_research_detail_bp = ctx.read_df("top_grant_depts") \
    .inner_map_join(ctx.read_df("top_pub_depts"), ["dept_code"], rsuffix = "pub") \
    .to_wf_spec()

utils.info("  top_dept_research_detail: {} op(s)".format(len(top_dept_research_detail_bp.jobs_operations)))

# --- WF 177: top_dept_student_detail <- top_high_gpa_depts x top_senior_depts [on dept_code] ---
top_dept_student_detail_bp = ctx.read_df("top_high_gpa_depts") \
    .inner_map_join(ctx.read_df("top_senior_depts"), ["dept_code"], rsuffix = "senior") \
    .to_wf_spec()

utils.info("  top_dept_student_detail: {} op(s)".format(len(top_dept_student_detail_bp.jobs_operations)))

# --- WF 178: top_dept_club_detail <- top_high_gpa_depts x top_club_depts [on dept_code] ---
top_dept_club_detail_bp = ctx.read_df("top_high_gpa_depts") \
    .inner_map_join(ctx.read_df("top_club_depts"), ["dept_code"], rsuffix = "club") \
    .to_wf_spec()

utils.info("  top_dept_club_detail: {} op(s)".format(len(top_dept_club_detail_bp.jobs_operations)))

# --- WF 179: top_course_analysis <- top_courses x top_high_gpa_courses [on course_id] ---
top_course_analysis_bp = ctx.read_df("top_courses") \
    .inner_map_join(ctx.read_df("top_high_gpa_courses"), ["course_id"], rsuffix = "hgpa") \
    .to_wf_spec()

utils.info("  top_course_analysis: {} op(s)".format(len(top_course_analysis_bp.jobs_operations)))

# --- WF 180: top_cs_analysis <- top_cs_courses x top_courses [on course_id] ---
top_cs_analysis_bp = ctx.read_df("top_cs_courses") \
    .inner_map_join(ctx.read_df("top_courses"), ["course_id"], rsuffix = "all") \
    .to_wf_spec()

utils.info("  top_cs_analysis:    {} op(s)".format(len(top_cs_analysis_bp.jobs_operations)))

# --- WF 181-190: Additional deep analytics with selects and filters ---

# --- WF 181: dept_performance_summary <- dept_performance_compare [add derived metrics] ---
dept_performance_summary_bp = ctx.read_df("dept_performance_compare") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  dept_performance_summary: {} op(s)".format(len(dept_performance_summary_bp.jobs_operations)))

# --- WF 182: student_engagement_summary <- student_engagement [add derived metrics] ---
student_engagement_summary_bp = ctx.read_df("student_engagement") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  student_engagement_summary: {} op(s)".format(len(student_engagement_summary_bp.jobs_operations)))

# --- WF 183: faculty_productivity_summary <- faculty_productivity [add derived metrics] ---
faculty_productivity_summary_bp = ctx.read_df("faculty_productivity") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  faculty_productivity_summary: {} op(s)".format(len(faculty_productivity_summary_bp.jobs_operations)))

# --- WF 184: cs_demand_summary <- cs_demand_analysis [filter top] ---
cs_demand_summary_bp = ctx.read_df("cs_demand_analysis") \
    .le_int("rank", 3) \
    .to_wf_spec()

utils.info("  cs_demand_summary:  {} op(s)".format(len(cs_demand_summary_bp.jobs_operations)))

# --- WF 185: student_advisor_summary <- student_advisor_load [filter top] ---
student_advisor_summary_bp = ctx.read_df("student_advisor_load") \
    .le_int("rank", 10) \
    .to_wf_spec()

utils.info("  student_advisor_summary: {} op(s)".format(len(student_advisor_summary_bp.jobs_operations)))

# --- WF 186: dept_resource_summary <- dept_resource_util [add derived metrics] ---
dept_resource_summary_bp = ctx.read_df("dept_resource_util") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  dept_resource_summary: {} op(s)".format(len(dept_resource_summary_bp.jobs_operations)))

# --- WF 187: dept_research_summary <- dept_research_compare [add derived metrics] ---
dept_research_summary_bp = ctx.read_df("dept_research_compare") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  dept_research_summary: {} op(s)".format(len(dept_research_summary_bp.jobs_operations)))

# --- WF 188: course_popularity_summary <- course_popularity_compare [add derived metrics] ---
course_popularity_summary_bp = ctx.read_df("course_popularity_compare") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  course_popularity_summary: {} op(s)".format(len(course_popularity_summary_bp.jobs_operations)))

# --- WF 189: upper_faculty_summary <- upper_faculty_ratio [add derived metrics] ---
upper_faculty_summary_bp = ctx.read_df("upper_faculty_ratio") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  upper_faculty_summary: {} op(s)".format(len(upper_faculty_summary_bp.jobs_operations)))

# --- WF 190: intern_advisor_summary <- dept_intern_advisor_ratio [add derived metrics] ---
intern_advisor_summary_bp = ctx.read_df("dept_intern_advisor_ratio") \
    .sort(["group:count"], [False]) \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  intern_advisor_summary: {} op(s)".format(len(intern_advisor_summary_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 9 — Final executive reports (10 WFs): WFs 191-200
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 9: Final executive reports (10 WFs) ---")

# --- WF 191: university_research_excellence <- top_dept_research_detail [final report] ---
university_research_excellence_bp = ctx.read_df("top_dept_research_detail") \
    .select(["dept_code", "dept_name", "group:count", "group:count:pub"]) \
    .checkpoint("research_excellence") \
    .to_wf_spec()

utils.info("  university_research_excellence: {} op(s)".format(len(university_research_excellence_bp.jobs_operations)))

# --- WF 192: university_student_success <- top_dept_student_detail [final report] ---
university_student_success_bp = ctx.read_df("top_dept_student_detail") \
    .select(["dept_code", "dept_name", "group:count", "group:count:senior"]) \
    .checkpoint("student_success") \
    .to_wf_spec()

utils.info("  university_student_success: {} op(s)".format(len(university_student_success_bp.jobs_operations)))

# --- WF 193: university_engagement <- top_dept_club_detail [final report] ---
university_engagement_bp = ctx.read_df("top_dept_club_detail") \
    .select(["dept_code", "dept_name", "group:count", "group:count:club"]) \
    .checkpoint("engagement") \
    .to_wf_spec()

utils.info("  university_engagement: {} op(s)".format(len(university_engagement_bp.jobs_operations)))

# --- WF 194: university_course_excellence <- top_course_analysis [final report] ---
university_course_excellence_bp = ctx.read_df("top_course_analysis") \
    .select(["course_id", "course_name", "group:count", "rank"]) \
    .checkpoint("course_excellence") \
    .to_wf_spec()

utils.info("  university_course_excellence: {} op(s)".format(len(university_course_excellence_bp.jobs_operations)))

# --- WF 195: university_cs_leadership <- top_cs_analysis [final report] ---
university_cs_leadership_bp = ctx.read_df("top_cs_analysis") \
    .select(["course_id", "course_name", "group:count", "rank"]) \
    .checkpoint("cs_leadership") \
    .to_wf_spec()

utils.info("  university_cs_leadership: {} op(s)".format(len(university_cs_leadership_bp.jobs_operations)))

# --- WF 196: university_dept_performance <- dept_performance_summary [final report] ---
university_dept_performance_bp = ctx.read_df("dept_performance_summary") \
    .le_int("rank", 5) \
    .checkpoint("dept_performance") \
    .to_wf_spec()

utils.info("  university_dept_performance: {} op(s)".format(len(university_dept_performance_bp.jobs_operations)))

# --- WF 197: university_faculty_impact <- faculty_productivity_summary [final report] ---
university_faculty_impact_bp = ctx.read_df("faculty_productivity_summary") \
    .le_int("rank", 10) \
    .checkpoint("faculty_impact") \
    .to_wf_spec()

utils.info("  university_faculty_impact: {} op(s)".format(len(university_faculty_impact_bp.jobs_operations)))

# --- WF 198: university_student_engagement <- student_engagement_summary [final report] ---
university_student_engagement_bp = ctx.read_df("student_engagement_summary") \
    .le_int("rank", 10) \
    .checkpoint("student_engagement_final") \
    .to_wf_spec()

utils.info("  university_student_engagement: {} op(s)".format(len(university_student_engagement_bp.jobs_operations)))

# --- WF 199: university_resource_efficiency <- dept_resource_summary [final report] ---
university_resource_efficiency_bp = ctx.read_df("dept_resource_summary") \
    .le_int("rank", 5) \
    .checkpoint("resource_efficiency") \
    .to_wf_spec()

utils.info("  university_resource_efficiency: {} op(s)".format(len(university_resource_efficiency_bp.jobs_operations)))

# --- WF 200: university_executive_summary <- Combined final dashboard ---
# Reads from multiple Phase 9 checkpoints and combines key metrics
university_executive_summary_bp = ctx.read_df("university_research_excellence") \
    .select(["dept_code", "dept_name"]) \
    .checkpoint("executive_summary") \
    .to_wf_spec()

utils.info("  university_executive_summary: {} op(s)".format(len(university_executive_summary_bp.jobs_operations)))

# ============================================================
# 6. Materializing SWF (mode={})
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("6. Materializing SWF (mode={})".format(args.mode))
utils.info("=" * 70)

swf_id = (
    SWFBuilder(ctx, params = swf_params)
        # Phase 0 (15 WFs, 5s buckets)
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
        .add_wf("classrooms", classrooms_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("schedules", schedules_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("prerequisites", prerequisites_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("research_grants", research_grants_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("publications", publications_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("clubs", clubs_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("club_members", club_members_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("advisors", advisors_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("internships", internships_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("library_books", library_books_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        # Phase 1 (20 WFs, 5s buckets)
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
        .add_wf("large_rooms", large_rooms_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("fall_schedules", fall_schedules_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("course_id_name", course_id_name_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("student_id_name", student_id_name_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("faculty_id_name", faculty_id_name_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("high_gpa_students", high_gpa_students_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("senior_students", senior_students_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("dept_budget_lookup", dept_budget_lookup_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("club_lookup", club_lookup_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("large_grants", large_grants_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("cs_students", cs_students_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("recent_pubs", recent_pubs_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("research_advisors", research_advisors_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("high_stipend_interns", high_stipend_interns_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("low_avail_books", low_avail_books_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        # Phase 2 (25 WFs, 10s buckets)
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
        .add_wf("schedule_room", schedule_room_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("schedule_course", schedule_course_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("prereq_names", prereq_names_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("faculty_grants", faculty_grants_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("faculty_pubs", faculty_pubs_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("member_club", member_club_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("member_student", member_student_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("high_gpa_dept", high_gpa_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("senior_dept", senior_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("club_dept", club_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("book_dept", book_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("advisor_faculty", advisor_faculty_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("advisor_student", advisor_student_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("intern_student", intern_student_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("cs_enrollment", cs_enrollment_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("upper_course_sched", upper_course_sched_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("faculty_dept_select", faculty_dept_select_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("enrollment_slim", enrollment_slim_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("grant_faculty_dept", grant_faculty_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("schedule_faculty", schedule_faculty_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        # Phase 3 (25 WFs, 10s buckets)
        .add_wf("high_gpa_enrollment", high_gpa_enrollment_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("senior_enrollment", senior_enrollment_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("upper_enrollment", upper_enrollment_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("grant_dept", grant_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("pub_dept", pub_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("member_club_student", member_club_student_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("high_gpa_course_enrich", high_gpa_course_enrich_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("senior_course_enrich", senior_course_enrich_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("upper_student_enrich", upper_student_enrich_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("club_dept_budget", club_dept_budget_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("schedule_full", schedule_full_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("advisor_dept", advisor_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("intern_dept", intern_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("intern_grade", intern_grade_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("prereq_count", prereq_count_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("club_member_count", club_member_count_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("student_club_count", student_club_count_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("book_dept_budget", book_dept_budget_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("cs_course_enroll", cs_course_enroll_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("schedule_room_faculty", schedule_room_faculty_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("book_count", book_count_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("advisor_count", advisor_count_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("intern_count", intern_count_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("transcript_slim", transcript_slim_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("student_grade_select", student_grade_select_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        # Phase 4 (25 WFs, 10s buckets)
        .add_wf("student_load", student_load_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("course_demand", course_demand_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("dept_enrollment", dept_enrollment_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("grant_by_dept", grant_by_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("pub_by_dept", pub_by_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("pub_by_faculty", pub_by_faculty_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("grant_by_faculty", grant_by_faculty_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("high_gpa_by_dept", high_gpa_by_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("senior_by_dept", senior_by_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("high_gpa_by_course", high_gpa_by_course_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("senior_by_course", senior_by_course_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("upper_by_student", upper_by_student_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("upper_by_dept", upper_by_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("schedule_by_building", schedule_by_building_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("club_by_dept", club_by_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("advisor_by_dept", advisor_by_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("intern_by_company", intern_by_company_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("intern_by_dept", intern_by_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("book_by_dept", book_by_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("advisor_by_student", advisor_by_student_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("grade_distribution", grade_distribution_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("course_by_level", course_by_level_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("student_by_year", student_by_year_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("cs_by_course", cs_by_course_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("faculty_by_dept", faculty_by_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        # Phase 5 (25 WFs, 20s buckets)
        .add_wf("student_load_name", student_load_name_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("course_demand_name", course_demand_name_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("pub_faculty_name", pub_faculty_name_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("grant_faculty_name", grant_faculty_name_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("advisor_student_name", advisor_student_name_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("student_load_sorted", student_load_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("course_demand_sorted", course_demand_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("high_load_students", high_load_students_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("popular_courses", popular_courses_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("active_advisors", active_advisors_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("grade_dist_sorted", grade_dist_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("intern_dept_name", intern_dept_name_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("book_dept_name", book_dept_name_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("student_year_label", student_year_label_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("course_level_label", course_level_label_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("intern_company_count", intern_company_count_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("upper_dept_name", upper_dept_name_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("grant_dept_sorted", grant_dept_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("pub_dept_sorted", pub_dept_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("high_gpa_dept_sorted", high_gpa_dept_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("senior_dept_sorted", senior_dept_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("club_dept_sorted", club_dept_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("cs_course_sorted", cs_course_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("high_gpa_course_sorted", high_gpa_course_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("senior_course_sorted", senior_course_sorted_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        # Phase 6 (25 WFs, 20s buckets)
        .add_wf("dept_grant_pub", dept_grant_pub_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_students_clubs", dept_students_clubs_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("student_intern_club", student_intern_club_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("course_demand_prereq", course_demand_prereq_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("faculty_grant_pub", faculty_grant_pub_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_enrollment_books", dept_enrollment_books_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_interns_advisors", dept_interns_advisors_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("student_load_advisor", student_load_advisor_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("cs_courses_demand", cs_courses_demand_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("high_gpa_senior_courses", high_gpa_senior_courses_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_senior_high_gpa", dept_senior_high_gpa_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("upper_dept_faculty", upper_dept_faculty_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("schedule_building_summary", schedule_building_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_intern_companies", top_intern_companies_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("combined_year_level", combined_year_level_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_students", top_students_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_courses", top_courses_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_grant_depts", top_grant_depts_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_pub_depts", top_pub_depts_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_high_gpa_depts", top_high_gpa_depts_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_senior_depts", top_senior_depts_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_club_depts", top_club_depts_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_cs_courses", top_cs_courses_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_high_gpa_courses", top_high_gpa_courses_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_senior_courses", top_senior_courses_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        # Phase 7 (15 WFs, 20s buckets)
        .add_wf("dept_research_compare", dept_research_compare_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_student_compare", dept_student_compare_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("course_popularity_compare", course_popularity_compare_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_performance_compare", dept_performance_compare_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("student_engagement", student_engagement_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("faculty_productivity", faculty_productivity_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("cs_demand_analysis", cs_demand_analysis_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("student_advisor_load", student_advisor_load_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_resource_util", dept_resource_util_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_intern_advisor_ratio", dept_intern_advisor_ratio_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("upper_faculty_ratio", upper_faculty_ratio_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_student_summary", top_student_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_course_summary", top_course_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("grade_dist_analysis", grade_dist_analysis_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("course_prereq_popularity", course_prereq_popularity_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        # Phase 8 (15 WFs, 20s buckets)
        .add_wf("top_dept_research_detail", top_dept_research_detail_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_dept_student_detail", top_dept_student_detail_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_dept_club_detail", top_dept_club_detail_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_course_analysis", top_course_analysis_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("top_cs_analysis", top_cs_analysis_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_performance_summary", dept_performance_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("student_engagement_summary", student_engagement_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("faculty_productivity_summary", faculty_productivity_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("cs_demand_summary", cs_demand_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("student_advisor_summary", student_advisor_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_resource_summary", dept_resource_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_research_summary", dept_research_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("course_popularity_summary", course_popularity_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("upper_faculty_summary", upper_faculty_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("intern_advisor_summary", intern_advisor_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        # Phase 9 (10 WFs, 20s buckets)
        .add_wf("university_research_excellence", university_research_excellence_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("university_student_success", university_student_success_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("university_engagement", university_engagement_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("university_course_excellence", university_course_excellence_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("university_cs_leadership", university_cs_leadership_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("university_dept_performance", university_dept_performance_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("university_faculty_impact", university_faculty_impact_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("university_student_engagement", university_student_engagement_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("university_resource_efficiency", university_resource_efficiency_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("university_executive_summary", university_executive_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
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

# all output_ids to read
output_names = [
    # Phase 0
    "students", "courses", "enrollments", "faculty", "departments",
    "classrooms", "schedules", "prerequisites", "research_grants",
    "publications", "clubs", "club_members", "advisors", "internships", "library_books",
    # Phase 1
    "active_students", "upper_courses", "passing_grades", "tenured_faculty", "dept_lookup",
    "large_rooms", "fall_schedules", "course_id_name", "student_id_name", "faculty_id_name",
    "high_gpa_students", "senior_students", "dept_budget_lookup", "club_lookup",
    "large_grants", "cs_students", "recent_pubs", "research_advisors", "high_stipend_interns", "low_avail_books",
    # Phase 2
    "student_grades", "course_enrollment", "faculty_dept", "student_dept",
    "full_transcript", "schedule_room", "schedule_course", "prereq_names",
    "faculty_grants", "faculty_pubs", "member_club", "member_student",
    "high_gpa_dept", "senior_dept", "club_dept", "book_dept",
    "advisor_faculty", "advisor_student", "intern_student", "cs_enrollment",
    "upper_course_sched", "faculty_dept_select", "enrollment_slim", "grant_faculty_dept", "schedule_faculty",
    # Phase 3
    "high_gpa_enrollment", "senior_enrollment", "upper_enrollment", "grant_dept", "pub_dept",
    "member_club_student", "high_gpa_course_enrich", "senior_course_enrich", "upper_student_enrich",
    "club_dept_budget", "schedule_full", "advisor_dept", "intern_dept", "intern_grade",
    "prereq_count", "club_member_count", "student_club_count", "book_dept_budget",
    "cs_course_enroll", "schedule_room_faculty", "book_count", "advisor_count", "intern_count",
    "transcript_slim", "student_grade_select",
    # Phase 4
    "student_load", "course_demand", "dept_enrollment", "grant_by_dept", "pub_by_dept",
    "pub_by_faculty", "grant_by_faculty", "high_gpa_by_dept", "senior_by_dept",
    "high_gpa_by_course", "senior_by_course", "upper_by_student", "upper_by_dept",
    "schedule_by_building", "club_by_dept", "advisor_by_dept", "intern_by_company",
    "intern_by_dept", "book_by_dept", "advisor_by_student", "grade_distribution",
    "course_by_level", "student_by_year", "cs_by_course", "faculty_by_dept",
    # Phase 5
    "student_load_name", "course_demand_name", "pub_faculty_name", "grant_faculty_name",
    "advisor_student_name", "student_load_sorted", "course_demand_sorted",
    "high_load_students", "popular_courses", "active_advisors", "grade_dist_sorted",
    "intern_dept_name", "book_dept_name", "student_year_label", "course_level_label",
    "intern_company_count", "upper_dept_name", "grant_dept_sorted", "pub_dept_sorted",
    "high_gpa_dept_sorted", "senior_dept_sorted", "club_dept_sorted", "cs_course_sorted",
    "high_gpa_course_sorted", "senior_course_sorted",
    # Phase 6
    "dept_grant_pub", "dept_students_clubs", "student_intern_club", "course_demand_prereq",
    "faculty_grant_pub", "dept_enrollment_books", "dept_interns_advisors", "student_load_advisor",
    "cs_courses_demand", "high_gpa_senior_courses", "dept_senior_high_gpa", "upper_dept_faculty",
    "schedule_building_summary", "top_intern_companies", "combined_year_level",
    "top_students", "top_courses", "top_grant_depts", "top_pub_depts",
    "top_high_gpa_depts", "top_senior_depts", "top_club_depts", "top_cs_courses",
    "top_high_gpa_courses", "top_senior_courses",
    # Phase 7
    "dept_research_compare", "dept_student_compare", "course_popularity_compare",
    "dept_performance_compare", "student_engagement", "faculty_productivity",
    "cs_demand_analysis", "student_advisor_load", "dept_resource_util",
    "dept_intern_advisor_ratio", "upper_faculty_ratio", "top_student_summary",
    "top_course_summary", "grade_dist_analysis", "course_prereq_popularity",
    # Phase 8
    "top_dept_research_detail", "top_dept_student_detail", "top_dept_club_detail",
    "top_course_analysis", "top_cs_analysis", "dept_performance_summary",
    "student_engagement_summary", "faculty_productivity_summary", "cs_demand_summary",
    "student_advisor_summary", "dept_resource_summary", "dept_research_summary",
    "course_popularity_summary", "upper_faculty_summary", "intern_advisor_summary",
    # Phase 9
    "university_research_excellence", "university_student_success", "university_engagement",
    "university_course_excellence", "university_cs_leadership", "university_dept_performance",
    "university_faculty_impact", "university_student_engagement",
    "university_resource_efficiency", "university_executive_summary",
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

# Print sample results from each phase
utils.info("")
utils.info("  --- Phase 0: Root datasets ---")
for name in ["students", "courses", "enrollments", "faculty", "departments",
             "classrooms", "schedules", "prerequisites", "research_grants",
             "publications", "clubs", "club_members", "advisors", "internships", "library_books"]:
    r = results[name]
    utils.info("  {:30s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 9: Executive reports ---")
for name in ["university_research_excellence", "university_student_success", "university_engagement",
             "university_course_excellence", "university_cs_leadership", "university_dept_performance",
             "university_faculty_impact", "university_student_engagement",
             "university_resource_efficiency", "university_executive_summary"]:
    r = results[name]
    utils.info("  {:30s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

# ============================================================
# Summary
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("DONE - University analytics SWF completed (200 WFs, 10 phases)")
utils.info("=" * 70)
utils.info("")
utils.info("  Phase breakdown:")
utils.info("    Phase 0: 15 root datasets")
utils.info("    Phase 1: 20 filters & transforms")
utils.info("    Phase 2: 25 primary joins")
utils.info("    Phase 3: 25 secondary enrichment")
utils.info("    Phase 4: 25 aggregations")
utils.info("    Phase 5: 25 tertiary analytics")
utils.info("    Phase 6: 25 cross-domain analytics")
utils.info("    Phase 7: 15 comparative reports")
utils.info("    Phase 8: 15 deep analytics")
utils.info("    Phase 9: 10 final executive reports")
