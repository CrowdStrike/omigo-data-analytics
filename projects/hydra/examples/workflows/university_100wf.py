from omigo_hydra import cluster_services_v2, cluster_common_v2
from omigo_hydra import cluster_protocol_v2
from omigo_hydra.cluster_services_v2 import SWFBuilder
from omigo_core import utils
import sys, os, argparse


"""Example: University analytics SWF with 100 WFs — comprehensive DAG patterns.
Demonstrates:
  1. Multiple root WFs (12 independent data sources)
  2. Complex fan-out (students feeds 10+ downstream WFs)
  3. Complex fan-in (reports read from 5+ upstream WFs)
  4. Deep diamond patterns (8 phases, 0-7)
  5. Multi-phase processing (8 phases)
  6. Variable bucket sizes (5s -> 10s -> 20s)
  7. ctx.from_maps() for root WFs with inline synthetic data
  8. ctx.read_df("upstream") for downstream WFs (proxy DataFrame, auto-resolved)
  9. inner_map_join with rsuffix for column conflict resolution
  10. Checkpoint (intermediate output_id)
  11. BVar broadcast variables (bctx.min_gpa, bctx.min_capacity)
  12. SWF params (semester, report_date)
  13. read_df("upstream", params = ["key"], prefix = "ctx") for param enrichment
  14. group_count aggregation
  15. select (column pruning)
  16. drop_cols, rename, sort, add_seq_num for final reports
  17. Edge inference — no manual input_ids/output_ids anywhere

DAG (100 WFs, 8 phases):

  Phase 0 — Root datasets (12 WFs, data sources)
  Phase 1 — Basic filters & transforms (15 WFs)
  Phase 2 — Primary joins (20 WFs)
  Phase 3 — Secondary enrichment (18 WFs)
  Phase 4 — Aggregations (15 WFs)
  Phase 5 — Cross-domain analytics (10 WFs)
  Phase 6 — Comparative reports (6 WFs)
  Phase 7 — Final executive reports (4 WFs)

Run:
  python university_100wf.py --mode local
  python university_100wf.py --mode cluster
"""

# Command Line
parser = argparse.ArgumentParser(description = "SWF example: university analytics (100 WFs, 8 phases)")
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
    cluster_services_v2.init("university_100wf")
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

utils.info("  min_gpa = {}".format(bctx.min_gpa.value))
utils.info("  min_capacity = {}".format(bctx.min_capacity.value))

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
}
utils.info("  params: {}".format(swf_params))

# ============================================================
# 5. Define WF blueprints (100 WFs in 8 phases)
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("5. Defining WF blueprints (100 WFs)")
utils.info("=" * 70)

# ==============================================================================
# COLUMN TRACKING LEGEND
# Each WF definition is preceded by a comment showing its output columns.
# This is the authoritative reference for downstream WFs.
# ==============================================================================

# ------------------------------------------------------------------
# Phase 0 — Root WFs (12 WFs): inline synthetic data via ctx.from_maps()
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 0: Root datasets (12 WFs) ---")

# --- WF 1: students (root, 20 rows) ---
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
    {"student_id": "S020", "student_name": "Tina", "gpa": "2.8", "dept_code": "BIO", "year": "1", "status": "active"},
]).to_wf_spec()

utils.info("  students:           {} op(s)  [root, 20 rows]".format(len(students_bp.jobs_operations)))

# --- WF 2: courses (root, 15 rows) ---
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
]).to_wf_spec()

utils.info("  courses:            {} op(s)  [root, 15 rows]".format(len(courses_bp.jobs_operations)))

# --- WF 3: enrollments (root, 40 rows) ---
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
]).to_wf_spec()

utils.info("  enrollments:        {} op(s)  [root, 40 rows]".format(len(enrollments_bp.jobs_operations)))

# --- WF 4: faculty (root, 12 rows) ---
# OUTPUT COLS: faculty_id, faculty_name, dept_code, title
faculty_bp = ctx.from_maps([
    {"faculty_id": "F01", "faculty_name": "Dr. Smith", "dept_code": "CS", "title": "Professor"},
    {"faculty_id": "F02", "faculty_name": "Dr. Johnson", "dept_code": "CS", "title": "Associate"},
    {"faculty_id": "F03", "faculty_name": "Dr. Williams", "dept_code": "MATH", "title": "Professor"},
    {"faculty_id": "F04", "faculty_name": "Dr. Brown", "dept_code": "PHYS", "title": "Adjunct"},
    {"faculty_id": "F05", "faculty_name": "Dr. Davis", "dept_code": "PHYS", "title": "Professor"},
    {"faculty_id": "F06", "faculty_name": "Dr. Miller", "dept_code": "CHEM", "title": "Associate"},
    {"faculty_id": "F07", "faculty_name": "Dr. Wilson", "dept_code": "BIO", "title": "Professor"},
    {"faculty_id": "F08", "faculty_name": "Dr. Moore", "dept_code": "MATH", "title": "Adjunct"},
    {"faculty_id": "F09", "faculty_name": "Dr. Taylor", "dept_code": "CS", "title": "Assistant"},
    {"faculty_id": "F10", "faculty_name": "Dr. Anderson", "dept_code": "PHYS", "title": "Associate"},
    {"faculty_id": "F11", "faculty_name": "Dr. Thomas", "dept_code": "BIO", "title": "Assistant"},
    {"faculty_id": "F12", "faculty_name": "Dr. Jackson", "dept_code": "CHEM", "title": "Professor"},
]).to_wf_spec()

utils.info("  faculty:            {} op(s)  [root, 12 rows]".format(len(faculty_bp.jobs_operations)))

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

# --- WF 6: classrooms (root, 10 rows) ---
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
]).to_wf_spec()

utils.info("  classrooms:         {} op(s)  [root, 10 rows]".format(len(classrooms_bp.jobs_operations)))

# --- WF 7: schedules (root, 20 rows) ---
# OUTPUT COLS: schedule_id, course_id, room_id, time_slot, semester
schedules_bp = ctx.from_maps([
    {"schedule_id": "SCH001", "course_id": "C301", "room_id": "R101", "time_slot": "MWF 9-10", "semester": "Fall2026"},
    {"schedule_id": "SCH002", "course_id": "C302", "room_id": "R101", "time_slot": "TTH 10-12", "semester": "Fall2026"},
    {"schedule_id": "SCH003", "course_id": "C201", "room_id": "R102", "time_slot": "MWF 2-3", "semester": "Fall2026"},
    {"schedule_id": "SCH004", "course_id": "C110", "room_id": "R201", "time_slot": "MWF 9-10", "semester": "Fall2026"},
    {"schedule_id": "SCH005", "course_id": "C210", "room_id": "R202", "time_slot": "TTH 1-2", "semester": "Fall2026"},
    {"schedule_id": "SCH006", "course_id": "C320", "room_id": "R301", "time_slot": "MWF 11-12", "semester": "Fall2026"},
    {"schedule_id": "SCH007", "course_id": "C120", "room_id": "R301", "time_slot": "TTH 9-11", "semester": "Fall2026"},
    {"schedule_id": "SCH008", "course_id": "C130", "room_id": "R401", "time_slot": "MWF 2-4", "semester": "Fall2026"},
    {"schedule_id": "SCH009", "course_id": "C140", "room_id": "R501", "time_slot": "TTH 10-11", "semester": "Fall2026"},
    {"schedule_id": "SCH010", "course_id": "C101", "room_id": "R101", "time_slot": "MWF 1-2", "semester": "Fall2026"},
    {"schedule_id": "SCH011", "course_id": "C401", "room_id": "R101", "time_slot": "TTH 2-4", "semester": "Fall2026"},
    {"schedule_id": "SCH012", "course_id": "C310", "room_id": "R201", "time_slot": "MWF 3-4", "semester": "Fall2026"},
    {"schedule_id": "SCH013", "course_id": "C220", "room_id": "R301", "time_slot": "TTH 1-3", "semester": "Fall2026"},
    {"schedule_id": "SCH014", "course_id": "C230", "room_id": "R401", "time_slot": "MWF 10-12", "semester": "Fall2026"},
    {"schedule_id": "SCH015", "course_id": "C240", "room_id": "R501", "time_slot": "TTH 11-12", "semester": "Fall2026"},
    {"schedule_id": "SCH016", "course_id": "C301", "room_id": "R102", "time_slot": "F 2-5", "semester": "Fall2026"},
    {"schedule_id": "SCH017", "course_id": "C210", "room_id": "R202", "time_slot": "W 3-5", "semester": "Fall2026"},
    {"schedule_id": "SCH018", "course_id": "C120", "room_id": "R302", "time_slot": "T 2-5", "semester": "Fall2026"},
    {"schedule_id": "SCH019", "course_id": "C140", "room_id": "R502", "time_slot": "TH 2-5", "semester": "Fall2026"},
    {"schedule_id": "SCH020", "course_id": "C401", "room_id": "R601", "time_slot": "M 6-9", "semester": "Fall2026"},
]).to_wf_spec()

utils.info("  schedules:          {} op(s)  [root, 20 rows]".format(len(schedules_bp.jobs_operations)))

# --- WF 8: prerequisites (root, 12 rows) ---
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
]).to_wf_spec()

utils.info("  prerequisites:      {} op(s)  [root, 12 rows]".format(len(prerequisites_bp.jobs_operations)))

# --- WF 9: research_grants (root, 8 rows) ---
# OUTPUT COLS: grant_id, faculty_id, amount, grant_title, grant_year
research_grants_bp = ctx.from_maps([
    {"grant_id": "G001", "faculty_id": "F01", "amount": "500000", "grant_title": "AI Safety Research", "grant_year": "2026"},
    {"grant_id": "G002", "faculty_id": "F01", "amount": "300000", "grant_title": "ML Optimization", "grant_year": "2026"},
    {"grant_id": "G003", "faculty_id": "F03", "amount": "250000", "grant_title": "Number Theory Apps", "grant_year": "2026"},
    {"grant_id": "G004", "faculty_id": "F05", "amount": "600000", "grant_title": "Quantum Computing", "grant_year": "2026"},
    {"grant_id": "G005", "faculty_id": "F07", "amount": "400000", "grant_title": "Genomics Research", "grant_year": "2026"},
    {"grant_id": "G006", "faculty_id": "F12", "amount": "350000", "grant_title": "Green Chemistry", "grant_year": "2026"},
    {"grant_id": "G007", "faculty_id": "F02", "amount": "200000", "grant_title": "Data Mining", "grant_year": "2026"},
    {"grant_id": "G008", "faculty_id": "F10", "amount": "450000", "grant_title": "Particle Physics", "grant_year": "2026"},
]).to_wf_spec()

utils.info("  research_grants:    {} op(s)  [root, 8 rows]".format(len(research_grants_bp.jobs_operations)))

# --- WF 10: publications (root, 15 rows) ---
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
]).to_wf_spec()

utils.info("  publications:       {} op(s)  [root, 15 rows]".format(len(publications_bp.jobs_operations)))

# --- WF 11: clubs (root, 10 rows) ---
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
]).to_wf_spec()

utils.info("  clubs:              {} op(s)  [root, 10 rows]".format(len(clubs_bp.jobs_operations)))

# --- WF 12: club_members (root, 25 rows) ---
# OUTPUT COLS: student_id, club_id, role
club_members_bp = ctx.from_maps([
    {"student_id": "S001", "club_id": "CL01", "role": "president"},
    {"student_id": "S001", "club_id": "CL09", "role": "member"},
    {"student_id": "S005", "club_id": "CL01", "role": "vice-president"},
    {"student_id": "S005", "club_id": "CL06", "role": "member"},
    {"student_id": "S013", "club_id": "CL01", "role": "member"},
    {"student_id": "S017", "club_id": "CL01", "role": "treasurer"},
    {"student_id": "S017", "club_id": "CL09", "role": "president"},
    {"student_id": "S002", "club_id": "CL02", "role": "member"},
    {"student_id": "S006", "club_id": "CL02", "role": "president"},
    {"student_id": "S004", "club_id": "CL03", "role": "member"},
    {"student_id": "S007", "club_id": "CL03", "role": "president"},
    {"student_id": "S007", "club_id": "CL08", "role": "member"},
    {"student_id": "S019", "club_id": "CL03", "role": "member"},
    {"student_id": "S008", "club_id": "CL04", "role": "member"},
    {"student_id": "S016", "club_id": "CL04", "role": "vice-president"},
    {"student_id": "S009", "club_id": "CL05", "role": "president"},
    {"student_id": "S011", "club_id": "CL05", "role": "member"},
    {"student_id": "S015", "club_id": "CL05", "role": "treasurer"},
    {"student_id": "S020", "club_id": "CL05", "role": "member"},
    {"student_id": "S011", "club_id": "CL10", "role": "member"},
    {"student_id": "S013", "club_id": "CL06", "role": "member"},
    {"student_id": "S005", "club_id": "CL09", "role": "member"},
    {"student_id": "S014", "club_id": "CL03", "role": "treasurer"},
    {"student_id": "S018", "club_id": "CL02", "role": "member"},
    {"student_id": "S019", "club_id": "CL08", "role": "president"},
]).to_wf_spec()

utils.info("  club_members:       {} op(s)  [root, 25 rows]".format(len(club_members_bp.jobs_operations)))
# ------------------------------------------------------------------
# Phase 1 — Filters & transforms (15 WFs): WFs 13-27
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 1: Filters & transforms (15 WFs) ---")

# --- WF 13: active_students <- students ---
# Filter active students with GPA >= min_gpa, checkpoint, sort
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status
active_students_bp = ctx.read_df("students") \
    .eq_str("status", "active") \
    .ge_float("gpa", bctx.min_gpa.value) \
    .checkpoint("active_ckpt") \
    .sort("gpa") \
    .to_wf_spec()

utils.info("  active_students:    {} op(s)".format(len(active_students_bp.jobs_operations)))

# --- WF 14: upper_courses <- courses ---
# Filter level >= 300, checkpoint
# OUTPUT COLS: course_id, course_name, dept_code, credits, level
upper_courses_bp = ctx.read_df("courses") \
    .ge_float("level", 300.0) \
    .checkpoint("upper_ckpt") \
    .to_wf_spec()

utils.info("  upper_courses:      {} op(s)".format(len(upper_courses_bp.jobs_operations)))

# --- WF 15: passing_grades <- enrollments ---
# Exclude grade=F
# OUTPUT COLS: student_id, course_id, grade, semester
passing_grades_bp = ctx.read_df("enrollments") \
    .not_eq_str("grade", "F") \
    .to_wf_spec()

utils.info("  passing_grades:     {} op(s)".format(len(passing_grades_bp.jobs_operations)))

# --- WF 16: tenured_faculty <- faculty ---
# Exclude title=Adjunct
# OUTPUT COLS: faculty_id, faculty_name, dept_code, title
tenured_faculty_bp = ctx.read_df("faculty") \
    .not_eq_str("title", "Adjunct") \
    .to_wf_spec()

utils.info("  tenured_faculty:    {} op(s)".format(len(tenured_faculty_bp.jobs_operations)))

# --- WF 17: dept_lookup <- departments ---
# Slim: only dept_code + dept_name
# OUTPUT COLS: dept_code, dept_name
dept_lookup_bp = ctx.read_df("departments") \
    .select(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  dept_lookup:        {} op(s)".format(len(dept_lookup_bp.jobs_operations)))

# --- WF 18: large_rooms <- classrooms ---
# Filter capacity >= min_capacity
# OUTPUT COLS: room_id, building, capacity, room_type
large_rooms_bp = ctx.read_df("classrooms") \
    .ge_float("capacity", bctx.min_capacity.value) \
    .to_wf_spec()

utils.info("  large_rooms:        {} op(s)".format(len(large_rooms_bp.jobs_operations)))

# --- WF 19: fall_schedules <- schedules ---
# Filter semester=Fall2026
# OUTPUT COLS: schedule_id, course_id, room_id, time_slot, semester
fall_schedules_bp = ctx.read_df("schedules") \
    .eq_str("semester", "Fall2026") \
    .to_wf_spec()

utils.info("  fall_schedules:     {} op(s)".format(len(fall_schedules_bp.jobs_operations)))

# --- WF 20: course_id_name <- courses ---
# Slim: only course_id + course_name (for clean joins)
# OUTPUT COLS: course_id, course_name
course_id_name_bp = ctx.read_df("courses") \
    .select(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  course_id_name:     {} op(s)".format(len(course_id_name_bp.jobs_operations)))

# --- WF 21: student_id_name <- students ---
# Slim: student_id + student_name only
# OUTPUT COLS: student_id, student_name
student_id_name_bp = ctx.read_df("students") \
    .select(["student_id", "student_name"]) \
    .to_wf_spec()

utils.info("  student_id_name:    {} op(s)".format(len(student_id_name_bp.jobs_operations)))

# --- WF 22: faculty_id_name <- faculty ---
# Slim: faculty_id + faculty_name only
# OUTPUT COLS: faculty_id, faculty_name
faculty_id_name_bp = ctx.read_df("faculty") \
    .select(["faculty_id", "faculty_name"]) \
    .to_wf_spec()

utils.info("  faculty_id_name:    {} op(s)".format(len(faculty_id_name_bp.jobs_operations)))

# --- WF 23: high_gpa_students <- students ---
# Filter GPA >= 3.5
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status
high_gpa_students_bp = ctx.read_df("students") \
    .ge_float("gpa", 3.5) \
    .to_wf_spec()

utils.info("  high_gpa_students:  {} op(s)".format(len(high_gpa_students_bp.jobs_operations)))

# --- WF 24: senior_students <- students ---
# Filter year=4
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status
senior_students_bp = ctx.read_df("students") \
    .eq_str("year", "4") \
    .to_wf_spec()

utils.info("  senior_students:    {} op(s)".format(len(senior_students_bp.jobs_operations)))

# --- WF 25: dept_budget_lookup <- departments ---
# Slim: dept_code + budget
# OUTPUT COLS: dept_code, budget
dept_budget_lookup_bp = ctx.read_df("departments") \
    .select(["dept_code", "budget"]) \
    .to_wf_spec()

utils.info("  dept_budget_lookup: {} op(s)".format(len(dept_budget_lookup_bp.jobs_operations)))

# --- WF 26: club_lookup <- clubs ---
# Slim: club_id + club_name
# OUTPUT COLS: club_id, club_name
club_lookup_bp = ctx.read_df("clubs") \
    .select(["club_id", "club_name"]) \
    .to_wf_spec()

utils.info("  club_lookup:        {} op(s)".format(len(club_lookup_bp.jobs_operations)))

# --- WF 27: grant_lookup <- research_grants ---
# Slim: grant_id + faculty_id + amount
# OUTPUT COLS: grant_id, faculty_id, amount
grant_lookup_bp = ctx.read_df("research_grants") \
    .select(["grant_id", "faculty_id", "amount"]) \
    .to_wf_spec()

utils.info("  grant_lookup:       {} op(s)".format(len(grant_lookup_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 2 — Primary joins (20 WFs): WFs 28-47
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 2: Primary joins (20 WFs) ---")

# --- WF 28: student_grades <- active_students x passing_grades [on student_id] ---
# LEFT:  active_students  -> student_id, student_name, gpa, dept_code, year, status
# RIGHT: passing_grades   -> student_id, course_id, grade, semester
# Shared non-key: NONE (no conflicts)
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester
student_grades_bp = ctx.read_df("active_students") \
    .inner_map_join(ctx.read_df("passing_grades"), ["student_id"]) \
    .to_wf_spec()

utils.info("  student_grades:     {} op(s)".format(len(student_grades_bp.jobs_operations)))

# --- WF 29: course_enrollment <- passing_grades x course_id_name [on course_id] ---
# LEFT:  passing_grades  -> student_id, course_id, grade, semester
# RIGHT: course_id_name  -> course_id, course_name
# Shared non-key: NONE
# OUTPUT COLS: student_id, course_id, grade, semester, course_name
course_enrollment_bp = ctx.read_df("passing_grades") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  course_enrollment:  {} op(s)".format(len(course_enrollment_bp.jobs_operations)))

# --- WF 30: faculty_dept <- tenured_faculty x dept_lookup [on dept_code] ---
# LEFT:  tenured_faculty -> faculty_id, faculty_name, dept_code, title
# RIGHT: dept_lookup     -> dept_code, dept_name
# Shared non-key: NONE
# OUTPUT COLS: faculty_id, faculty_name, dept_code, title, dept_name
faculty_dept_bp = ctx.read_df("tenured_faculty") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  faculty_dept:       {} op(s)".format(len(faculty_dept_bp.jobs_operations)))

# --- WF 31: student_dept <- active_students x dept_lookup [on dept_code, param: semester] ---
# LEFT:  active_students (with param) -> student_id, student_name, gpa, dept_code, year, status, ctx:semester
# RIGHT: dept_lookup -> dept_code, dept_name
# Shared non-key: NONE
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, ctx:semester, dept_name
student_dept_bp = ctx.read_df("active_students", params = ["semester"], prefix = "ctx") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  student_dept:       {} op(s)".format(len(student_dept_bp.jobs_operations)))

# --- WF 32: full_transcript <- student_grades x courses [on course_id, rsuffix=crs] ---
# LEFT:  student_grades -> student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester
# RIGHT: courses        -> course_id, course_name, dept_code, credits, level
# Shared non-key: dept_code -> becomes dept_code:crs
# Also semester is not in courses, grade not in courses, etc. Only dept_code conflicts.
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester, course_name:crs, dept_code:crs, credits:crs, level:crs
full_transcript_bp = ctx.read_df("student_grades") \
    .inner_map_join(ctx.read_df("courses"), ["course_id"], rsuffix = "crs") \
    .to_wf_spec()

utils.info("  full_transcript:    {} op(s)".format(len(full_transcript_bp.jobs_operations)))

# --- WF 33: schedule_room <- fall_schedules x large_rooms [on room_id] ---
# LEFT:  fall_schedules -> schedule_id, course_id, room_id, time_slot, semester
# RIGHT: large_rooms    -> room_id, building, capacity, room_type
# Shared non-key: NONE
# OUTPUT COLS: schedule_id, course_id, room_id, time_slot, semester, building, capacity, room_type
schedule_room_bp = ctx.read_df("fall_schedules") \
    .inner_map_join(ctx.read_df("large_rooms"), ["room_id"]) \
    .to_wf_spec()

utils.info("  schedule_room:      {} op(s)".format(len(schedule_room_bp.jobs_operations)))

# --- WF 34: schedule_course <- fall_schedules x course_id_name [on course_id] ---
# LEFT:  fall_schedules -> schedule_id, course_id, room_id, time_slot, semester
# RIGHT: course_id_name -> course_id, course_name
# Shared non-key: NONE
# OUTPUT COLS: schedule_id, course_id, room_id, time_slot, semester, course_name
schedule_course_bp = ctx.read_df("fall_schedules") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  schedule_course:    {} op(s)".format(len(schedule_course_bp.jobs_operations)))

# --- WF 35: prereq_names <- prerequisites x course_id_name [on course_id] ---
# LEFT:  prerequisites  -> course_id, prereq_id
# RIGHT: course_id_name -> course_id, course_name
# Shared non-key: NONE
# OUTPUT COLS: course_id, prereq_id, course_name
prereq_names_bp = ctx.read_df("prerequisites") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  prereq_names:       {} op(s)".format(len(prereq_names_bp.jobs_operations)))

# --- WF 36: faculty_grants <- tenured_faculty x grant_lookup [on faculty_id] ---
# LEFT:  tenured_faculty -> faculty_id, faculty_name, dept_code, title
# RIGHT: grant_lookup    -> grant_id, faculty_id, amount
# Shared non-key: NONE
# OUTPUT COLS: faculty_id, faculty_name, dept_code, title, grant_id, amount
faculty_grants_bp = ctx.read_df("tenured_faculty") \
    .inner_map_join(ctx.read_df("grant_lookup"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  faculty_grants:     {} op(s)".format(len(faculty_grants_bp.jobs_operations)))

# --- WF 37: faculty_pubs <- faculty_id_name x publications [on faculty_id] ---
# LEFT:  faculty_id_name -> faculty_id, faculty_name
# RIGHT: publications    -> pub_id, faculty_id, pub_title, venue, pub_year
# Shared non-key: NONE
# OUTPUT COLS: faculty_id, faculty_name, pub_id, pub_title, venue, pub_year
faculty_pubs_bp = ctx.read_df("faculty_id_name") \
    .inner_map_join(ctx.read_df("publications"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  faculty_pubs:       {} op(s)".format(len(faculty_pubs_bp.jobs_operations)))

# --- WF 38: member_club <- club_members x club_lookup [on club_id] ---
# LEFT:  club_members -> student_id, club_id, role
# RIGHT: club_lookup  -> club_id, club_name
# Shared non-key: NONE
# OUTPUT COLS: student_id, club_id, role, club_name
member_club_bp = ctx.read_df("club_members") \
    .inner_map_join(ctx.read_df("club_lookup"), ["club_id"]) \
    .to_wf_spec()

utils.info("  member_club:        {} op(s)".format(len(member_club_bp.jobs_operations)))

# --- WF 39: member_student <- club_members x student_id_name [on student_id] ---
# LEFT:  club_members   -> student_id, club_id, role
# RIGHT: student_id_name -> student_id, student_name
# Shared non-key: NONE
# OUTPUT COLS: student_id, club_id, role, student_name
member_student_bp = ctx.read_df("club_members") \
    .inner_map_join(ctx.read_df("student_id_name"), ["student_id"]) \
    .to_wf_spec()

utils.info("  member_student:     {} op(s)".format(len(member_student_bp.jobs_operations)))

# --- WF 40: high_gpa_dept <- high_gpa_students x dept_lookup [on dept_code] ---
# LEFT:  high_gpa_students -> student_id, student_name, gpa, dept_code, year, status
# RIGHT: dept_lookup       -> dept_code, dept_name
# Shared non-key: NONE
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, dept_name
high_gpa_dept_bp = ctx.read_df("high_gpa_students") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  high_gpa_dept:      {} op(s)".format(len(high_gpa_dept_bp.jobs_operations)))

# --- WF 41: senior_dept <- senior_students x dept_lookup [on dept_code] ---
# LEFT:  senior_students -> student_id, student_name, gpa, dept_code, year, status
# RIGHT: dept_lookup     -> dept_code, dept_name
# Shared non-key: NONE
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, dept_name
senior_dept_bp = ctx.read_df("senior_students") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  senior_dept:        {} op(s)".format(len(senior_dept_bp.jobs_operations)))

# --- WF 42: club_dept <- clubs x dept_lookup [on dept_code] ---
# LEFT:  clubs       -> club_id, club_name, dept_code, club_budget
# RIGHT: dept_lookup -> dept_code, dept_name
# Shared non-key: NONE
# OUTPUT COLS: club_id, club_name, dept_code, club_budget, dept_name
club_dept_bp = ctx.read_df("clubs") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  club_dept:          {} op(s)".format(len(club_dept_bp.jobs_operations)))

# --- WF 43: dept_with_budget <- dept_lookup x dept_budget_lookup [on dept_code] ---
# LEFT:  dept_lookup        -> dept_code, dept_name
# RIGHT: dept_budget_lookup -> dept_code, budget
# Shared non-key: NONE
# OUTPUT COLS: dept_code, dept_name, budget
dept_with_budget_bp = ctx.read_df("dept_lookup") \
    .inner_map_join(ctx.read_df("dept_budget_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  dept_with_budget:   {} op(s)".format(len(dept_with_budget_bp.jobs_operations)))

# --- WF 44: upper_course_sched <- upper_courses x fall_schedules [on course_id, rsuffix=sched] ---
# LEFT:  upper_courses  -> course_id, course_name, dept_code, credits, level
# RIGHT: fall_schedules -> schedule_id, course_id, room_id, time_slot, semester
# Shared non-key: NONE  (no conflicts)
# OUTPUT COLS: course_id, course_name, dept_code, credits, level, schedule_id, room_id, time_slot, semester
upper_course_sched_bp = ctx.read_df("upper_courses") \
    .inner_map_join(ctx.read_df("fall_schedules"), ["course_id"]) \
    .to_wf_spec()

utils.info("  upper_course_sched: {} op(s)".format(len(upper_course_sched_bp.jobs_operations)))

# --- WF 45: student_select <- active_students [select student_id, student_name, gpa] ---
# OUTPUT COLS: student_id, student_name, gpa
student_select_bp = ctx.read_df("active_students") \
    .select(["student_id", "student_name", "gpa"]) \
    .to_wf_spec()

utils.info("  student_select:     {} op(s)".format(len(student_select_bp.jobs_operations)))

# --- WF 46: faculty_dept_select <- faculty_dept [select faculty_id, dept_code, dept_name] ---
# INPUT: faculty_dept -> faculty_id, faculty_name, dept_code, title, dept_name
# OUTPUT COLS: faculty_id, dept_code, dept_name
faculty_dept_select_bp = ctx.read_df("faculty_dept") \
    .select(["faculty_id", "dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  faculty_dept_select: {} op(s)".format(len(faculty_dept_select_bp.jobs_operations)))

# --- WF 47: enrollment_slim <- passing_grades [select student_id, course_id] ---
# OUTPUT COLS: student_id, course_id
enrollment_slim_bp = ctx.read_df("passing_grades") \
    .select(["student_id", "course_id"]) \
    .to_wf_spec()

utils.info("  enrollment_slim:    {} op(s)".format(len(enrollment_slim_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 3 — Secondary enrichment (18 WFs): WFs 48-65
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 3: Secondary enrichment (18 WFs) ---")

# --- WF 48: transcript_slim <- full_transcript [select student_id, course_id, grade, course_name:crs, credits:crs] ---
# INPUT: full_transcript -> student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester, course_name:crs, dept_code:crs, credits:crs, level:crs
# OUTPUT COLS: student_id, course_id, grade, course_name:crs, credits:crs
transcript_slim_bp = ctx.read_df("full_transcript") \
    .select(["student_id", "course_id", "grade", "course_name:crs", "credits:crs"]) \
    .to_wf_spec()

utils.info("  transcript_slim:    {} op(s)".format(len(transcript_slim_bp.jobs_operations)))

# --- WF 49: schedule_full <- schedule_course x large_rooms [on room_id, rsuffix=rm] ---
# LEFT:  schedule_course -> schedule_id, course_id, room_id, time_slot, semester, course_name
# RIGHT: large_rooms     -> room_id, building, capacity, room_type
# Shared non-key: NONE
# OUTPUT COLS: schedule_id, course_id, room_id, time_slot, semester, course_name, building, capacity, room_type
schedule_full_bp = ctx.read_df("schedule_course") \
    .inner_map_join(ctx.read_df("large_rooms"), ["room_id"]) \
    .to_wf_spec()

utils.info("  schedule_full:      {} op(s)".format(len(schedule_full_bp.jobs_operations)))

# --- WF 50: grant_dept <- faculty_grants x dept_lookup [on dept_code] ---
# LEFT:  faculty_grants -> faculty_id, faculty_name, dept_code, title, grant_id, amount
# RIGHT: dept_lookup    -> dept_code, dept_name
# Shared non-key: NONE  (title is only in left, dept_name only in right)
# OUTPUT COLS: faculty_id, faculty_name, dept_code, title, grant_id, amount, dept_name
grant_dept_bp = ctx.read_df("faculty_grants") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  grant_dept:         {} op(s)".format(len(grant_dept_bp.jobs_operations)))

# --- WF 51: pub_dept <- faculty_pubs x faculty_dept_select [on faculty_id] ---
# LEFT:  faculty_pubs       -> faculty_id, faculty_name, pub_id, pub_title, venue, pub_year
# RIGHT: faculty_dept_select -> faculty_id, dept_code, dept_name
# Shared non-key: NONE  (faculty_name only in left)
# OUTPUT COLS: faculty_id, faculty_name, pub_id, pub_title, venue, pub_year, dept_code, dept_name
pub_dept_bp = ctx.read_df("faculty_pubs") \
    .inner_map_join(ctx.read_df("faculty_dept_select"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  pub_dept:           {} op(s)".format(len(pub_dept_bp.jobs_operations)))

# --- WF 52: member_club_student <- member_club x student_id_name [on student_id] ---
# LEFT:  member_club    -> student_id, club_id, role, club_name
# RIGHT: student_id_name -> student_id, student_name
# Shared non-key: NONE
# OUTPUT COLS: student_id, club_id, role, club_name, student_name
member_club_student_bp = ctx.read_df("member_club") \
    .inner_map_join(ctx.read_df("student_id_name"), ["student_id"]) \
    .to_wf_spec()

utils.info("  member_club_student: {} op(s)".format(len(member_club_student_bp.jobs_operations)))

# --- WF 53: high_gpa_enrollment <- enrollment_slim x high_gpa_students [on student_id, rsuffix=hgpa] ---
# LEFT:  enrollment_slim    -> student_id, course_id
# RIGHT: high_gpa_students  -> student_id, student_name, gpa, dept_code, year, status
# Shared non-key: NONE
# OUTPUT COLS: student_id, course_id, student_name, gpa, dept_code, year, status
high_gpa_enrollment_bp = ctx.read_df("enrollment_slim") \
    .inner_map_join(ctx.read_df("high_gpa_students"), ["student_id"]) \
    .to_wf_spec()

utils.info("  high_gpa_enrollment: {} op(s)".format(len(high_gpa_enrollment_bp.jobs_operations)))

# --- WF 54: senior_enrollment <- enrollment_slim x senior_students [on student_id, rsuffix=sr] ---
# LEFT:  enrollment_slim  -> student_id, course_id
# RIGHT: senior_students  -> student_id, student_name, gpa, dept_code, year, status
# Shared non-key: NONE
# OUTPUT COLS: student_id, course_id, student_name, gpa, dept_code, year, status
senior_enrollment_bp = ctx.read_df("enrollment_slim") \
    .inner_map_join(ctx.read_df("senior_students"), ["student_id"]) \
    .to_wf_spec()

utils.info("  senior_enrollment:  {} op(s)".format(len(senior_enrollment_bp.jobs_operations)))

# --- WF 55: upper_enrollment <- enrollment_slim x upper_courses [on course_id, rsuffix=ucrs] ---
# LEFT:  enrollment_slim -> student_id, course_id
# RIGHT: upper_courses   -> course_id, course_name, dept_code, credits, level
# Shared non-key: NONE
# OUTPUT COLS: student_id, course_id, course_name, dept_code, credits, level
upper_enrollment_bp = ctx.read_df("enrollment_slim") \
    .inner_map_join(ctx.read_df("upper_courses"), ["course_id"]) \
    .to_wf_spec()

utils.info("  upper_enrollment:   {} op(s)".format(len(upper_enrollment_bp.jobs_operations)))

# --- WF 56: prereq_chain <- prereq_names x course_id_name [on prereq_id as course_id] ---
# We need prereq_id to be the join key. But course_id_name has course_id.
# Instead: select prereq_id + course_name from prereq_names, rename prereq_id
# Actually, let's just group prereqs by course
# prereq_names has: course_id, prereq_id, course_name
# OUTPUT COLS: course_id, course_name, group:count
prereq_count_bp = ctx.read_df("prereq_names") \
    .group_count(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  prereq_count:       {} op(s)".format(len(prereq_count_bp.jobs_operations)))

# --- WF 57: club_member_count <- member_club [group_count by club_id, club_name] ---
# INPUT: member_club -> student_id, club_id, role, club_name
# OUTPUT COLS: club_id, club_name, group:count
club_member_count_bp = ctx.read_df("member_club") \
    .group_count(["club_id", "club_name"]) \
    .to_wf_spec()

utils.info("  club_member_count:  {} op(s)".format(len(club_member_count_bp.jobs_operations)))

# --- WF 58: student_club_count <- member_club [group_count by student_id] ---
# INPUT: member_club -> student_id, club_id, role, club_name
# OUTPUT COLS: student_id, group:count
student_club_count_bp = ctx.read_df("member_club") \
    .group_count(["student_id"]) \
    .to_wf_spec()

utils.info("  student_club_count: {} op(s)".format(len(student_club_count_bp.jobs_operations)))

# --- WF 59: high_gpa_course_enrich <- high_gpa_enrollment x course_id_name [on course_id] ---
# LEFT:  high_gpa_enrollment -> student_id, course_id, student_name, gpa, dept_code, year, status
# RIGHT: course_id_name      -> course_id, course_name
# Shared non-key: NONE
# OUTPUT COLS: student_id, course_id, student_name, gpa, dept_code, year, status, course_name
high_gpa_course_enrich_bp = ctx.read_df("high_gpa_enrollment") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  high_gpa_course_enrich: {} op(s)".format(len(high_gpa_course_enrich_bp.jobs_operations)))

# --- WF 60: senior_course_enrich <- senior_enrollment x course_id_name [on course_id] ---
# LEFT:  senior_enrollment -> student_id, course_id, student_name, gpa, dept_code, year, status
# RIGHT: course_id_name    -> course_id, course_name
# Shared non-key: NONE
# OUTPUT COLS: student_id, course_id, student_name, gpa, dept_code, year, status, course_name
senior_course_enrich_bp = ctx.read_df("senior_enrollment") \
    .inner_map_join(ctx.read_df("course_id_name"), ["course_id"]) \
    .to_wf_spec()

utils.info("  senior_course_enrich: {} op(s)".format(len(senior_course_enrich_bp.jobs_operations)))

# --- WF 61: upper_student_enrich <- upper_enrollment x student_id_name [on student_id] ---
# LEFT:  upper_enrollment -> student_id, course_id, course_name, dept_code, credits, level
# RIGHT: student_id_name  -> student_id, student_name
# Shared non-key: NONE
# OUTPUT COLS: student_id, course_id, course_name, dept_code, credits, level, student_name
upper_student_enrich_bp = ctx.read_df("upper_enrollment") \
    .inner_map_join(ctx.read_df("student_id_name"), ["student_id"]) \
    .to_wf_spec()

utils.info("  upper_student_enrich: {} op(s)".format(len(upper_student_enrich_bp.jobs_operations)))

# --- WF 62: club_dept_budget <- club_dept x dept_budget_lookup [on dept_code] ---
# LEFT:  club_dept          -> club_id, club_name, dept_code, club_budget, dept_name
# RIGHT: dept_budget_lookup -> dept_code, budget
# Shared non-key: NONE
# OUTPUT COLS: club_id, club_name, dept_code, club_budget, dept_name, budget
club_dept_budget_bp = ctx.read_df("club_dept") \
    .inner_map_join(ctx.read_df("dept_budget_lookup"), ["dept_code"]) \
    .to_wf_spec()

utils.info("  club_dept_budget:   {} op(s)".format(len(club_dept_budget_bp.jobs_operations)))

# --- WF 63: student_grade_select <- student_grades [select student_id, course_id, grade] ---
# INPUT: student_grades -> student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester
# OUTPUT COLS: student_id, course_id, grade
student_grade_select_bp = ctx.read_df("student_grades") \
    .select(["student_id", "course_id", "grade"]) \
    .to_wf_spec()

utils.info("  student_grade_select: {} op(s)".format(len(student_grade_select_bp.jobs_operations)))

# --- WF 64: faculty_grant_select <- faculty_grants [select faculty_id, grant_id, amount] ---
# INPUT: faculty_grants -> faculty_id, faculty_name, dept_code, title, grant_id, amount
# OUTPUT COLS: faculty_id, grant_id, amount
faculty_grant_select_bp = ctx.read_df("faculty_grants") \
    .select(["faculty_id", "grant_id", "amount"]) \
    .to_wf_spec()

utils.info("  faculty_grant_select: {} op(s)".format(len(faculty_grant_select_bp.jobs_operations)))

# --- WF 65: schedule_slim <- schedule_full [select course_id, room_id, building, capacity] ---
# INPUT: schedule_full -> schedule_id, course_id, room_id, time_slot, semester, course_name, building, capacity, room_type
# OUTPUT COLS: course_id, room_id, building, capacity
schedule_slim_bp = ctx.read_df("schedule_full") \
    .select(["course_id", "room_id", "building", "capacity"]) \
    .to_wf_spec()

utils.info("  schedule_slim:      {} op(s)".format(len(schedule_slim_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 4 — Aggregations (15 WFs): WFs 66-80
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 4: Aggregations (15 WFs) ---")

# --- WF 66: student_load <- full_transcript [group_count by student_id, student_name] ---
# INPUT: full_transcript -> student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester, course_name:crs, dept_code:crs, credits:crs, level:crs
# OUTPUT COLS: student_id, student_name, group:count
student_load_bp = ctx.read_df("full_transcript") \
    .group_count(["student_id", "student_name"]) \
    .to_wf_spec()

utils.info("  student_load:       {} op(s)".format(len(student_load_bp.jobs_operations)))

# --- WF 67: course_demand <- course_enrollment [group_count by course_id, course_name] ---
# INPUT: course_enrollment -> student_id, course_id, grade, semester, course_name
# OUTPUT COLS: course_id, course_name, group:count
course_demand_bp = ctx.read_df("course_enrollment") \
    .group_count(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  course_demand:      {} op(s)".format(len(course_demand_bp.jobs_operations)))

# --- WF 68: dept_enrollment <- student_dept [group_count by dept_code, dept_name] ---
# INPUT: student_dept -> student_id, student_name, gpa, dept_code, year, status, ctx:semester, dept_name
# OUTPUT COLS: dept_code, dept_name, group:count
dept_enrollment_bp = ctx.read_df("student_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  dept_enrollment:    {} op(s)".format(len(dept_enrollment_bp.jobs_operations)))

# --- WF 69: grant_by_dept <- grant_dept [group_count by dept_code, dept_name] ---
# INPUT: grant_dept -> faculty_id, faculty_name, dept_code, title, grant_id, amount, dept_name
# OUTPUT COLS: dept_code, dept_name, group:count
grant_by_dept_bp = ctx.read_df("grant_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  grant_by_dept:      {} op(s)".format(len(grant_by_dept_bp.jobs_operations)))

# --- WF 70: pub_by_dept <- pub_dept [group_count by dept_code, dept_name] ---
# INPUT: pub_dept -> faculty_id, faculty_name, pub_id, pub_title, venue, pub_year, dept_code, dept_name
# OUTPUT COLS: dept_code, dept_name, group:count
pub_by_dept_bp = ctx.read_df("pub_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  pub_by_dept:        {} op(s)".format(len(pub_by_dept_bp.jobs_operations)))

# --- WF 71: pub_by_faculty <- faculty_pubs [group_count by faculty_id, faculty_name] ---
# INPUT: faculty_pubs -> faculty_id, faculty_name, pub_id, pub_title, venue, pub_year
# OUTPUT COLS: faculty_id, faculty_name, group:count
pub_by_faculty_bp = ctx.read_df("faculty_pubs") \
    .group_count(["faculty_id", "faculty_name"]) \
    .to_wf_spec()

utils.info("  pub_by_faculty:     {} op(s)".format(len(pub_by_faculty_bp.jobs_operations)))

# --- WF 72: grant_by_faculty <- faculty_grants [group_count by faculty_id, faculty_name] ---
# INPUT: faculty_grants -> faculty_id, faculty_name, dept_code, title, grant_id, amount
# OUTPUT COLS: faculty_id, faculty_name, group:count
grant_by_faculty_bp = ctx.read_df("faculty_grants") \
    .group_count(["faculty_id", "faculty_name"]) \
    .to_wf_spec()

utils.info("  grant_by_faculty:   {} op(s)".format(len(grant_by_faculty_bp.jobs_operations)))

# --- WF 73: high_gpa_by_dept <- high_gpa_dept [group_count by dept_code, dept_name] ---
# INPUT: high_gpa_dept -> student_id, student_name, gpa, dept_code, year, status, dept_name
# OUTPUT COLS: dept_code, dept_name, group:count
high_gpa_by_dept_bp = ctx.read_df("high_gpa_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  high_gpa_by_dept:   {} op(s)".format(len(high_gpa_by_dept_bp.jobs_operations)))

# --- WF 74: senior_by_dept <- senior_dept [group_count by dept_code, dept_name] ---
# INPUT: senior_dept -> student_id, student_name, gpa, dept_code, year, status, dept_name
# OUTPUT COLS: dept_code, dept_name, group:count
senior_by_dept_bp = ctx.read_df("senior_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  senior_by_dept:     {} op(s)".format(len(senior_by_dept_bp.jobs_operations)))

# --- WF 75: high_gpa_by_course <- high_gpa_course_enrich [group_count by course_id, course_name] ---
# INPUT: high_gpa_course_enrich -> student_id, course_id, student_name, gpa, dept_code, year, status, course_name
# OUTPUT COLS: course_id, course_name, group:count
high_gpa_by_course_bp = ctx.read_df("high_gpa_course_enrich") \
    .group_count(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  high_gpa_by_course: {} op(s)".format(len(high_gpa_by_course_bp.jobs_operations)))

# --- WF 76: senior_by_course <- senior_course_enrich [group_count by course_id, course_name] ---
# INPUT: senior_course_enrich -> student_id, course_id, student_name, gpa, dept_code, year, status, course_name
# OUTPUT COLS: course_id, course_name, group:count
senior_by_course_bp = ctx.read_df("senior_course_enrich") \
    .group_count(["course_id", "course_name"]) \
    .to_wf_spec()

utils.info("  senior_by_course:   {} op(s)".format(len(senior_by_course_bp.jobs_operations)))

# --- WF 77: upper_by_student <- upper_student_enrich [group_count by student_id, student_name] ---
# INPUT: upper_student_enrich -> student_id, course_id, course_name, dept_code, credits, level, student_name
# OUTPUT COLS: student_id, student_name, group:count
upper_by_student_bp = ctx.read_df("upper_student_enrich") \
    .group_count(["student_id", "student_name"]) \
    .to_wf_spec()

utils.info("  upper_by_student:   {} op(s)".format(len(upper_by_student_bp.jobs_operations)))

# --- WF 78: upper_by_dept <- upper_student_enrich [group_count by dept_code] ---
# INPUT: upper_student_enrich -> student_id, course_id, course_name, dept_code, credits, level, student_name
# OUTPUT COLS: dept_code, group:count
upper_by_dept_bp = ctx.read_df("upper_student_enrich") \
    .group_count(["dept_code"]) \
    .to_wf_spec()

utils.info("  upper_by_dept:      {} op(s)".format(len(upper_by_dept_bp.jobs_operations)))

# --- WF 79: schedule_by_building <- schedule_full [group_count by building] ---
# INPUT: schedule_full -> schedule_id, course_id, room_id, time_slot, semester, course_name, building, capacity, room_type
# OUTPUT COLS: building, group:count
schedule_by_building_bp = ctx.read_df("schedule_full") \
    .group_count(["building"]) \
    .to_wf_spec()

utils.info("  schedule_by_building: {} op(s)".format(len(schedule_by_building_bp.jobs_operations)))

# --- WF 80: club_by_dept <- club_dept [group_count by dept_code, dept_name] ---
# INPUT: club_dept -> club_id, club_name, dept_code, club_budget, dept_name
# OUTPUT COLS: dept_code, dept_name, group:count
club_by_dept_bp = ctx.read_df("club_dept") \
    .group_count(["dept_code", "dept_name"]) \
    .to_wf_spec()

utils.info("  club_by_dept:       {} op(s)".format(len(club_by_dept_bp.jobs_operations)))
# ------------------------------------------------------------------
# Phase 5 — Cross-domain analytics (10 WFs): WFs 81-90
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 5: Cross-domain analytics (10 WFs) ---")

# --- WF 81: honor_roll <- student_load x student_select [on student_id] ---
# LEFT:  student_load   -> student_id, student_name, group:count
# RIGHT: student_select -> student_id, student_name, gpa
# Shared non-key: student_name -> rsuffix needed
# OUTPUT COLS: student_id, student_name, group:count, student_name:hnr, gpa:hnr
# Then drop student_name:hnr, rename group:count -> course_count
# FINAL OUTPUT COLS: student_id, student_name, course_count, gpa:hnr
honor_roll_bp = ctx.read_df("student_load") \
    .inner_map_join(ctx.read_df("student_select"), ["student_id"], rsuffix = "hnr") \
    .drop_cols(["student_name:hnr"]) \
    .rename("group:count", "course_count") \
    .sort("gpa:hnr") \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  honor_roll:         {} op(s)".format(len(honor_roll_bp.jobs_operations)))

# --- WF 82: dept_report <- dept_enrollment x faculty_dept [on dept_code, rsuffix=fac] ---
# LEFT:  dept_enrollment -> dept_code, dept_name, group:count
# RIGHT: faculty_dept    -> faculty_id, faculty_name, dept_code, title, dept_name
# Shared non-key: dept_name -> rsuffix needed
# OUTPUT COLS: dept_code, dept_name, group:count, faculty_id:fac, faculty_name:fac, title:fac, dept_name:fac
# Then drop dept_name:fac, rename group:count -> student_count
# FINAL OUTPUT COLS: dept_code, dept_name, student_count, faculty_id:fac, faculty_name:fac, title:fac
dept_report_bp = ctx.read_df("dept_enrollment") \
    .inner_map_join(ctx.read_df("faculty_dept"), ["dept_code"], rsuffix = "fac") \
    .drop_cols(["dept_name:fac"]) \
    .rename("group:count", "student_count") \
    .sort("dept_code") \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  dept_report:        {} op(s)".format(len(dept_report_bp.jobs_operations)))

# --- WF 83: grant_report <- grant_by_dept x dept_with_budget [on dept_code, rsuffix=bdgt] ---
# LEFT:  grant_by_dept   -> dept_code, dept_name, group:count
# RIGHT: dept_with_budget -> dept_code, dept_name, budget
# Shared non-key: dept_name -> rsuffix needed
# OUTPUT COLS: dept_code, dept_name, group:count, dept_name:bdgt, budget:bdgt
# Then drop dept_name:bdgt, rename group:count -> grant_count
# FINAL OUTPUT COLS: dept_code, dept_name, grant_count, budget:bdgt
grant_report_bp = ctx.read_df("grant_by_dept") \
    .inner_map_join(ctx.read_df("dept_with_budget"), ["dept_code"], rsuffix = "bdgt") \
    .drop_cols(["dept_name:bdgt"]) \
    .rename("group:count", "grant_count") \
    .sort("dept_code") \
    .to_wf_spec()

utils.info("  grant_report:       {} op(s)".format(len(grant_report_bp.jobs_operations)))

# --- WF 84: pub_report <- pub_by_dept x dept_with_budget [on dept_code, rsuffix=bdgt2] ---
# LEFT:  pub_by_dept     -> dept_code, dept_name, group:count
# RIGHT: dept_with_budget -> dept_code, dept_name, budget
# Shared non-key: dept_name -> rsuffix needed
# OUTPUT COLS: dept_code, dept_name, group:count, dept_name:bdgt2, budget:bdgt2
# Then drop dept_name:bdgt2, rename group:count -> pub_count
# FINAL OUTPUT COLS: dept_code, dept_name, pub_count, budget:bdgt2
pub_report_bp = ctx.read_df("pub_by_dept") \
    .inner_map_join(ctx.read_df("dept_with_budget"), ["dept_code"], rsuffix = "bdgt2") \
    .drop_cols(["dept_name:bdgt2"]) \
    .rename("group:count", "pub_count") \
    .sort("dept_code") \
    .to_wf_spec()

utils.info("  pub_report:         {} op(s)".format(len(pub_report_bp.jobs_operations)))

# --- WF 85: faculty_output <- pub_by_faculty x grant_by_faculty [on faculty_id, rsuffix=gr] ---
# LEFT:  pub_by_faculty   -> faculty_id, faculty_name, group:count
# RIGHT: grant_by_faculty -> faculty_id, faculty_name, group:count
# Shared non-key: faculty_name, group:count -> rsuffix needed
# OUTPUT COLS: faculty_id, faculty_name, group:count, faculty_name:gr, group:count:gr
# Then drop faculty_name:gr, rename group:count -> pub_count, rename group:count:gr -> grant_count
# FINAL OUTPUT COLS: faculty_id, faculty_name, pub_count, grant_count
faculty_output_bp = ctx.read_df("pub_by_faculty") \
    .inner_map_join(ctx.read_df("grant_by_faculty"), ["faculty_id"], rsuffix = "gr") \
    .drop_cols(["faculty_name:gr"]) \
    .rename("group:count", "pub_count") \
    .rename("group:count:gr", "grant_count") \
    .sort("faculty_id") \
    .to_wf_spec()

utils.info("  faculty_output:     {} op(s)".format(len(faculty_output_bp.jobs_operations)))

# --- WF 86: high_gpa_dept_report <- high_gpa_by_dept x dept_with_budget [on dept_code, rsuffix=bdgt3] ---
# LEFT:  high_gpa_by_dept -> dept_code, dept_name, group:count
# RIGHT: dept_with_budget -> dept_code, dept_name, budget
# Shared non-key: dept_name -> rsuffix needed
# Then drop dept_name:bdgt3, rename group:count -> honors_count
# FINAL OUTPUT COLS: dept_code, dept_name, honors_count, budget:bdgt3
high_gpa_dept_report_bp = ctx.read_df("high_gpa_by_dept") \
    .inner_map_join(ctx.read_df("dept_with_budget"), ["dept_code"], rsuffix = "bdgt3") \
    .drop_cols(["dept_name:bdgt3"]) \
    .rename("group:count", "honors_count") \
    .sort("dept_code") \
    .to_wf_spec()

utils.info("  high_gpa_dept_report: {} op(s)".format(len(high_gpa_dept_report_bp.jobs_operations)))

# --- WF 87: senior_dept_report <- senior_by_dept x dept_with_budget [on dept_code, rsuffix=bdgt4] ---
# LEFT:  senior_by_dept   -> dept_code, dept_name, group:count
# RIGHT: dept_with_budget -> dept_code, dept_name, budget
# Shared non-key: dept_name -> rsuffix needed
# Then drop dept_name:bdgt4, rename group:count -> senior_count
# FINAL OUTPUT COLS: dept_code, dept_name, senior_count, budget:bdgt4
senior_dept_report_bp = ctx.read_df("senior_by_dept") \
    .inner_map_join(ctx.read_df("dept_with_budget"), ["dept_code"], rsuffix = "bdgt4") \
    .drop_cols(["dept_name:bdgt4"]) \
    .rename("group:count", "senior_count") \
    .sort("dept_code") \
    .to_wf_spec()

utils.info("  senior_dept_report: {} op(s)".format(len(senior_dept_report_bp.jobs_operations)))

# --- WF 88: club_report <- club_member_count x club_dept_budget [on club_id, rsuffix=cdb] ---
# LEFT:  club_member_count -> club_id, club_name, group:count
# RIGHT: club_dept_budget  -> club_id, club_name, dept_code, club_budget, dept_name, budget
# Shared non-key: club_name -> rsuffix needed
# OUTPUT COLS: club_id, club_name, group:count, club_name:cdb, dept_code:cdb, club_budget:cdb, dept_name:cdb, budget:cdb
# Then drop club_name:cdb, rename group:count -> member_count
# FINAL OUTPUT COLS: club_id, club_name, member_count, dept_code:cdb, club_budget:cdb, dept_name:cdb, budget:cdb
club_report_bp = ctx.read_df("club_member_count") \
    .inner_map_join(ctx.read_df("club_dept_budget"), ["club_id"], rsuffix = "cdb") \
    .drop_cols(["club_name:cdb"]) \
    .rename("group:count", "member_count") \
    .sort("club_id") \
    .to_wf_spec()

utils.info("  club_report:        {} op(s)".format(len(club_report_bp.jobs_operations)))

# --- WF 89: student_activity <- student_club_count x student_select [on student_id, rsuffix=sa] ---
# LEFT:  student_club_count -> student_id, group:count
# RIGHT: student_select     -> student_id, student_name, gpa
# Shared non-key: NONE
# OUTPUT COLS: student_id, group:count, student_name, gpa
# Then rename group:count -> club_count
# FINAL OUTPUT COLS: student_id, club_count, student_name, gpa
student_activity_bp = ctx.read_df("student_club_count") \
    .inner_map_join(ctx.read_df("student_select"), ["student_id"]) \
    .rename("group:count", "club_count") \
    .sort("student_id") \
    .to_wf_spec()

utils.info("  student_activity:   {} op(s)".format(len(student_activity_bp.jobs_operations)))

# --- WF 90: schedule_report <- schedule_by_building x dept_with_budget [on building as ... ] ---
# schedule_by_building has: building, group:count. No dept_code for join.
# Instead: rename group:count -> class_count, sort
# FINAL OUTPUT COLS: building, class_count
schedule_report_bp = ctx.read_df("schedule_by_building") \
    .rename("group:count", "class_count") \
    .sort("building") \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  schedule_report:    {} op(s)".format(len(schedule_report_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 6 — Comparative reports (6 WFs): WFs 91-96
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 6: Comparative reports (6 WFs) ---")

# --- WF 91: dept_students_vs_honors <- dept_enrollment x high_gpa_by_dept [on dept_code, rsuffix=hon] ---
# LEFT:  dept_enrollment  -> dept_code, dept_name, group:count
# RIGHT: high_gpa_by_dept -> dept_code, dept_name, group:count
# Shared non-key: dept_name, group:count -> rsuffix needed
# OUTPUT COLS: dept_code, dept_name, group:count, dept_name:hon, group:count:hon
# Then drop dept_name:hon, rename group:count -> total_students, rename group:count:hon -> honors_students
# FINAL OUTPUT COLS: dept_code, dept_name, total_students, honors_students
dept_students_vs_honors_bp = ctx.read_df("dept_enrollment") \
    .inner_map_join(ctx.read_df("high_gpa_by_dept"), ["dept_code"], rsuffix = "hon") \
    .drop_cols(["dept_name:hon"]) \
    .rename("group:count", "total_students") \
    .rename("group:count:hon", "honors_students") \
    .sort("dept_code") \
    .to_wf_spec()

utils.info("  dept_students_vs_honors: {} op(s)".format(len(dept_students_vs_honors_bp.jobs_operations)))

# --- WF 92: dept_grants_vs_pubs <- grant_by_dept x pub_by_dept [on dept_code, rsuffix=pub] ---
# LEFT:  grant_by_dept -> dept_code, dept_name, group:count
# RIGHT: pub_by_dept   -> dept_code, dept_name, group:count
# Shared non-key: dept_name, group:count -> rsuffix needed
# OUTPUT COLS: dept_code, dept_name, group:count, dept_name:pub, group:count:pub
# Then drop dept_name:pub, rename group:count -> grant_count, rename group:count:pub -> pub_count
# FINAL OUTPUT COLS: dept_code, dept_name, grant_count, pub_count
dept_grants_vs_pubs_bp = ctx.read_df("grant_by_dept") \
    .inner_map_join(ctx.read_df("pub_by_dept"), ["dept_code"], rsuffix = "pub") \
    .drop_cols(["dept_name:pub"]) \
    .rename("group:count", "grant_count") \
    .rename("group:count:pub", "pub_count") \
    .sort("dept_code") \
    .to_wf_spec()

utils.info("  dept_grants_vs_pubs: {} op(s)".format(len(dept_grants_vs_pubs_bp.jobs_operations)))

# --- WF 93: course_demand_ranked <- course_demand [rename, sort, seq_num] ---
# INPUT: course_demand -> course_id, course_name, group:count
# FINAL OUTPUT COLS: course_id, course_name, enrollment_count, rank
course_demand_ranked_bp = ctx.read_df("course_demand") \
    .rename("group:count", "enrollment_count") \
    .sort("enrollment_count") \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  course_demand_ranked: {} op(s)".format(len(course_demand_ranked_bp.jobs_operations)))

# --- WF 94: upper_dept_report <- upper_by_dept x dept_lookup [on dept_code] ---
# LEFT:  upper_by_dept -> dept_code, group:count
# RIGHT: dept_lookup   -> dept_code, dept_name
# Shared non-key: NONE
# OUTPUT COLS: dept_code, group:count, dept_name
# Then rename group:count -> upper_enrollment_count
# FINAL OUTPUT COLS: dept_code, upper_enrollment_count, dept_name
upper_dept_report_bp = ctx.read_df("upper_by_dept") \
    .inner_map_join(ctx.read_df("dept_lookup"), ["dept_code"]) \
    .rename("group:count", "upper_enrollment_count") \
    .sort("dept_code") \
    .to_wf_spec()

utils.info("  upper_dept_report:  {} op(s)".format(len(upper_dept_report_bp.jobs_operations)))

# --- WF 95: high_gpa_course_ranked <- high_gpa_by_course [rename, sort, seq_num] ---
# INPUT: high_gpa_by_course -> course_id, course_name, group:count
# FINAL OUTPUT COLS: course_id, course_name, honors_enrollment, rank
high_gpa_course_ranked_bp = ctx.read_df("high_gpa_by_course") \
    .rename("group:count", "honors_enrollment") \
    .sort("honors_enrollment") \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  high_gpa_course_ranked: {} op(s)".format(len(high_gpa_course_ranked_bp.jobs_operations)))

# --- WF 96: senior_course_ranked <- senior_by_course [rename, sort, seq_num] ---
# INPUT: senior_by_course -> course_id, course_name, group:count
# FINAL OUTPUT COLS: course_id, course_name, senior_enrollment, rank
senior_course_ranked_bp = ctx.read_df("senior_by_course") \
    .rename("group:count", "senior_enrollment") \
    .sort("senior_enrollment") \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  senior_course_ranked: {} op(s)".format(len(senior_course_ranked_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 7 — Final executive reports (4 WFs): WFs 97-100
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 7: Final executive reports (4 WFs) ---")

# --- WF 97: executive_dept_summary <- dept_students_vs_honors x dept_grants_vs_pubs [on dept_code, rsuffix=gp] ---
# LEFT:  dept_students_vs_honors -> dept_code, dept_name, total_students, honors_students
# RIGHT: dept_grants_vs_pubs     -> dept_code, dept_name, grant_count, pub_count
# Shared non-key: dept_name -> rsuffix needed
# OUTPUT COLS: dept_code, dept_name, total_students, honors_students, dept_name:gp, grant_count:gp, pub_count:gp
# Then drop dept_name:gp
# FINAL OUTPUT COLS: dept_code, dept_name, total_students, honors_students, grant_count:gp, pub_count:gp, rank
executive_dept_summary_bp = ctx.read_df("dept_students_vs_honors") \
    .inner_map_join(ctx.read_df("dept_grants_vs_pubs"), ["dept_code"], rsuffix = "gp") \
    .drop_cols(["dept_name:gp"]) \
    .sort("dept_code") \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  executive_dept_summary: {} op(s)".format(len(executive_dept_summary_bp.jobs_operations)))

# --- WF 98: executive_faculty_summary <- faculty_output x faculty_dept_select [on faculty_id] ---
# LEFT:  faculty_output      -> faculty_id, faculty_name, pub_count, grant_count
# RIGHT: faculty_dept_select -> faculty_id, dept_code, dept_name
# Shared non-key: NONE
# OUTPUT COLS: faculty_id, faculty_name, pub_count, grant_count, dept_code, dept_name
# FINAL OUTPUT COLS: faculty_id, faculty_name, pub_count, grant_count, dept_code, dept_name, rank
executive_faculty_summary_bp = ctx.read_df("faculty_output") \
    .inner_map_join(ctx.read_df("faculty_dept_select"), ["faculty_id"]) \
    .sort("faculty_id") \
    .add_seq_num("rank") \
    .to_wf_spec()

utils.info("  executive_faculty_summary: {} op(s)".format(len(executive_faculty_summary_bp.jobs_operations)))

# --- WF 99: executive_student_summary <- honor_roll x student_activity [on student_id, rsuffix=act] ---
# LEFT:  honor_roll       -> student_id, student_name, course_count, gpa:hnr, rank
# RIGHT: student_activity -> student_id, club_count, student_name, gpa
# Shared non-key: student_name -> rsuffix needed (also gpa vs gpa:hnr - gpa is unique, but student_name is shared)
# Actually: left has student_name; right has student_name -> conflict! rsuffix needed.
# Also: left has gpa:hnr (not gpa), right has gpa -> no conflict for gpa. But student_name conflicts.
# RIGHT suffixed cols: club_count:act, student_name:act, gpa:act
# OUTPUT COLS: student_id, student_name, course_count, gpa:hnr, rank, club_count:act, student_name:act, gpa:act
# Then drop student_name:act, drop gpa:act (redundant with gpa:hnr)
# FINAL OUTPUT COLS: student_id, student_name, course_count, gpa:hnr, rank, club_count:act
executive_student_summary_bp = ctx.read_df("honor_roll") \
    .inner_map_join(ctx.read_df("student_activity"), ["student_id"], rsuffix = "act") \
    .drop_cols(["student_name:act"]) \
    .drop_cols(["gpa:act"]) \
    .sort("gpa:hnr") \
    .to_wf_spec()

utils.info("  executive_student_summary: {} op(s)".format(len(executive_student_summary_bp.jobs_operations)))

# --- WF 100: executive_course_summary <- course_demand_ranked x prereq_count [on course_id, rsuffix=prq] ---
# LEFT:  course_demand_ranked -> course_id, course_name, enrollment_count, rank
# RIGHT: prereq_count         -> course_id, course_name, group:count
# Shared non-key: course_name -> rsuffix needed
# OUTPUT COLS: course_id, course_name, enrollment_count, rank, course_name:prq, group:count:prq
# Then drop course_name:prq, rename group:count:prq -> prereq_count_val
# FINAL OUTPUT COLS: course_id, course_name, enrollment_count, rank, prereq_count_val
executive_course_summary_bp = ctx.read_df("course_demand_ranked") \
    .inner_map_join(ctx.read_df("prereq_count"), ["course_id"], rsuffix = "prq") \
    .drop_cols(["course_name:prq"]) \
    .rename("group:count:prq", "prereq_count_val") \
    .sort("enrollment_count") \
    .to_wf_spec()

utils.info("  executive_course_summary: {} op(s)".format(len(executive_course_summary_bp.jobs_operations)))

# ============================================================
# 6. Materialize SWF
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("6. Materializing SWF (mode={})".format(args.mode))
utils.info("=" * 70)

swf_id = (
    SWFBuilder(ctx, params = swf_params)
        # Phase 0 — Roots (12 WFs, 5s buckets)
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
        # Phase 1 — Filters & transforms (15 WFs, 5s buckets)
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
        .add_wf("grant_lookup", grant_lookup_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        # Phase 2 — Primary joins (20 WFs, 10s buckets)
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
        .add_wf("dept_with_budget", dept_with_budget_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("upper_course_sched", upper_course_sched_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("student_select", student_select_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("faculty_dept_select", faculty_dept_select_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("enrollment_slim", enrollment_slim_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        # Phase 3 — Secondary enrichment (18 WFs, 10s buckets)
        .add_wf("transcript_slim", transcript_slim_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("schedule_full", schedule_full_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("grant_dept", grant_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("pub_dept", pub_dept_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("member_club_student", member_club_student_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("high_gpa_enrollment", high_gpa_enrollment_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("senior_enrollment", senior_enrollment_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("upper_enrollment", upper_enrollment_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("prereq_count", prereq_count_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("club_member_count", club_member_count_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("student_club_count", student_club_count_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("high_gpa_course_enrich", high_gpa_course_enrich_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("senior_course_enrich", senior_course_enrich_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("upper_student_enrich", upper_student_enrich_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("club_dept_budget", club_dept_budget_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("student_grade_select", student_grade_select_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("faculty_grant_select", faculty_grant_select_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("schedule_slim", schedule_slim_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        # Phase 4 — Aggregations (15 WFs, 10s buckets)
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
        # Phase 5 — Cross-domain analytics (10 WFs, 20s buckets)
        .add_wf("honor_roll", honor_roll_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_report", dept_report_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("grant_report", grant_report_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("pub_report", pub_report_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("faculty_output", faculty_output_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("high_gpa_dept_report", high_gpa_dept_report_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("senior_dept_report", senior_dept_report_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("club_report", club_report_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("student_activity", student_activity_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("schedule_report", schedule_report_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        # Phase 6 — Comparative reports (6 WFs, 20s buckets)
        .add_wf("dept_students_vs_honors", dept_students_vs_honors_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("dept_grants_vs_pubs", dept_grants_vs_pubs_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("course_demand_ranked", course_demand_ranked_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("upper_dept_report", upper_dept_report_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("high_gpa_course_ranked", high_gpa_course_ranked_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("senior_course_ranked", senior_course_ranked_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        # Phase 7 — Final executive reports (4 WFs, 20s buckets)
        .add_wf("executive_dept_summary", executive_dept_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("executive_faculty_summary", executive_faculty_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("executive_student_summary", executive_student_summary_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("executive_course_summary", executive_course_summary_bp,
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

# all output_ids to read (100 WF outputs + 2 checkpoints)
output_names = [
    # Phase 0 — Roots
    "students", "courses", "enrollments", "faculty", "departments",
    "classrooms", "schedules", "prerequisites", "research_grants",
    "publications", "clubs", "club_members",
    # Phase 1 — Filters
    "active_students", "active_ckpt", "upper_courses", "upper_ckpt",
    "passing_grades", "tenured_faculty", "dept_lookup",
    "large_rooms", "fall_schedules", "course_id_name", "student_id_name",
    "faculty_id_name", "high_gpa_students", "senior_students",
    "dept_budget_lookup", "club_lookup", "grant_lookup",
    # Phase 2 — Primary joins
    "student_grades", "course_enrollment", "faculty_dept", "student_dept",
    "full_transcript", "schedule_room", "schedule_course", "prereq_names",
    "faculty_grants", "faculty_pubs", "member_club", "member_student",
    "high_gpa_dept", "senior_dept", "club_dept", "dept_with_budget",
    "upper_course_sched", "student_select", "faculty_dept_select", "enrollment_slim",
    # Phase 3 — Secondary enrichment
    "transcript_slim", "schedule_full", "grant_dept", "pub_dept",
    "member_club_student", "high_gpa_enrollment", "senior_enrollment",
    "upper_enrollment", "prereq_count", "club_member_count", "student_club_count",
    "high_gpa_course_enrich", "senior_course_enrich", "upper_student_enrich",
    "club_dept_budget", "student_grade_select", "faculty_grant_select", "schedule_slim",
    # Phase 4 — Aggregations
    "student_load", "course_demand", "dept_enrollment", "grant_by_dept",
    "pub_by_dept", "pub_by_faculty", "grant_by_faculty", "high_gpa_by_dept",
    "senior_by_dept", "high_gpa_by_course", "senior_by_course",
    "upper_by_student", "upper_by_dept", "schedule_by_building", "club_by_dept",
    # Phase 5 — Cross-domain
    "honor_roll", "dept_report", "grant_report", "pub_report",
    "faculty_output", "high_gpa_dept_report", "senior_dept_report",
    "club_report", "student_activity", "schedule_report",
    # Phase 6 — Comparative
    "dept_students_vs_honors", "dept_grants_vs_pubs", "course_demand_ranked",
    "upper_dept_report", "high_gpa_course_ranked", "senior_course_ranked",
    # Phase 7 — Executive
    "executive_dept_summary", "executive_faculty_summary",
    "executive_student_summary", "executive_course_summary",
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
for name in ["students", "courses", "enrollments", "faculty", "departments",
             "classrooms", "schedules", "prerequisites", "research_grants",
             "publications", "clubs", "club_members"]:
    r = results[name]
    utils.info("  {:30s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 1: Filters & transforms ---")
for name in ["active_students", "active_ckpt", "upper_courses", "upper_ckpt",
             "passing_grades", "tenured_faculty", "dept_lookup",
             "large_rooms", "fall_schedules", "course_id_name", "student_id_name",
             "faculty_id_name", "high_gpa_students", "senior_students",
             "dept_budget_lookup", "club_lookup", "grant_lookup"]:
    r = results[name]
    utils.info("  {:30s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 2: Primary joins ---")
for name in ["student_grades", "course_enrollment", "faculty_dept", "student_dept",
             "full_transcript", "schedule_room", "schedule_course", "prereq_names",
             "faculty_grants", "faculty_pubs", "member_club", "member_student",
             "high_gpa_dept", "senior_dept", "club_dept", "dept_with_budget",
             "upper_course_sched", "student_select", "faculty_dept_select", "enrollment_slim"]:
    r = results[name]
    utils.info("  {:30s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 3: Secondary enrichment ---")
for name in ["transcript_slim", "schedule_full", "grant_dept", "pub_dept",
             "member_club_student", "high_gpa_enrollment", "senior_enrollment",
             "upper_enrollment", "prereq_count", "club_member_count", "student_club_count",
             "high_gpa_course_enrich", "senior_course_enrich", "upper_student_enrich",
             "club_dept_budget", "student_grade_select", "faculty_grant_select", "schedule_slim"]:
    r = results[name]
    utils.info("  {:30s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 4: Aggregations ---")
for name in ["student_load", "course_demand", "dept_enrollment", "grant_by_dept",
             "pub_by_dept", "pub_by_faculty", "grant_by_faculty", "high_gpa_by_dept",
             "senior_by_dept", "high_gpa_by_course", "senior_by_course",
             "upper_by_student", "upper_by_dept", "schedule_by_building", "club_by_dept"]:
    r = results[name]
    utils.info("  {:30s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 5: Cross-domain analytics ---")
for name in ["honor_roll", "dept_report", "grant_report", "pub_report",
             "faculty_output", "high_gpa_dept_report", "senior_dept_report",
             "club_report", "student_activity", "schedule_report"]:
    r = results[name]
    utils.info("  {:30s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 6: Comparative reports ---")
for name in ["dept_students_vs_honors", "dept_grants_vs_pubs", "course_demand_ranked",
             "upper_dept_report", "high_gpa_course_ranked", "senior_course_ranked"]:
    r = results[name]
    utils.info("  {:30s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

utils.info("")
utils.info("  --- Phase 7: Final executive reports ---")
for name in ["executive_dept_summary", "executive_faculty_summary",
             "executive_student_summary", "executive_course_summary"]:
    r = results[name]
    utils.info("  {:30s} {:3d} rows, cols: {}".format(name, r.num_rows(), r.get_columns()))

# ============================================================
# Summary
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("DONE - University analytics SWF completed (100 WFs, 8 phases)")
utils.info("=" * 70)
utils.info("")
utils.info("  Phase breakdown:")
utils.info("    Phase 0: 12 root datasets")
utils.info("    Phase 1: 15 filters & transforms")
utils.info("    Phase 2: 20 primary joins")
utils.info("    Phase 3: 18 secondary enrichment")
utils.info("    Phase 4: 15 aggregations")
utils.info("    Phase 5: 10 cross-domain analytics")
utils.info("    Phase 6:  6 comparative reports")
utils.info("    Phase 7:  4 final executive reports")
utils.info("    Total:  100 WFs")
utils.info("")
utils.info("  API features used:")
utils.info("    ctx.from_maps()        12 root WFs with inline data")
utils.info("    ctx.read_df()          88 downstream WFs with proxy DataFrames")
utils.info("    inner_map_join         30+ joins")
utils.info("    rsuffix                15+ joins with column conflict resolution")
utils.info("    checkpoint             2 checkpoints (active_ckpt, upper_ckpt)")
utils.info("    BVar                   min_gpa={}, min_capacity={}".format(bctx.min_gpa.value, bctx.min_capacity.value))
utils.info("    SWF params             semester={}, report_date={}".format(swf_params["semester"], swf_params["report_date"]))
utils.info("    param enrichment       student_dept (semester from params)")
utils.info("    group_count            15 aggregations")
utils.info("    select                 10+ column prunings")
utils.info("    drop_cols + rename     10+ final reports")
utils.info("    sort + add_seq_num     10+ ranked reports")
utils.info("")
