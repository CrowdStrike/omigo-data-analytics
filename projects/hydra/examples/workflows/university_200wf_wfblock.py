from omigo_hydra import cluster_services_v2
from omigo_hydra import cluster_protocol_v2
from omigo_hydra.cluster_services_v2 import SWFBuilder
from omigo_hydra_v2 import cluster_df, cluster_common_v2
from omigo_hydra_v2.cluster_class_reflection import hydra_class_for
from omigo_core import utils, dataframe
import sys, os, argparse


"""Example: University analytics (200wf equivalent) using WfBlock pattern.
Demonstrates how WorkflowBlockDF-style reusable blocks dramatically simplify
a large WF DAG by encapsulating repetitive multi-step patterns into named methods.

The original university_200wf.py defines 200 WFs with many repeated inline patterns:
  - filter active students (status=active, gpa >= threshold) — repeated 5+ times
  - join with dept_lookup for dept_name enrichment — repeated 15+ times
  - join with course_id_name for course enrichment — repeated 10+ times
  - group_count + sort for aggregation reports — repeated 20+ times
  - drop duplicate cols + rename + sort + rank for final reports — repeated 10+ times

With UniversityBlockDF, these patterns are defined ONCE and reused everywhere.
This collapses 200 WFs worth of logic into ~25 WFs while covering the same domains.

Key concepts:
  1. UniversityBlockDF extends dataframe.DataFrame (same pattern as WorkflowBlockDF)
  2. Each method encapsulates a reusable multi-step pipeline
  3. Methods that join with other WFs take ctx to access ctx.read_df()
  4. Usage: df.extend_class(UniversityBlockDF).method_name(...)
  5. Blocks compose: filter_active().enrich_with_dept(ctx).summarize_by_dept()

Domains covered (matching 200wf):
  - Students, courses, enrollments (core academics)
  - Faculty, research grants, publications (research)
  - Clubs, club memberships (student life)
  - Advisors (faculty-student relationships)
  - Internships (career outcomes)
  - Library books (resources)

DAG (25 WFs, 5 phases):
  Phase 0: 8 root WFs (students, courses, enrollments, faculty, departments,
           research_grants, clubs, internships)
  Phase 1: 5 filtered/lookup WFs (using blocks: filter_active, filter_upper, etc.)
  Phase 2: 5 join WFs (using blocks: enrich_with_dept, enrich_with_course, etc.)
  Phase 3: 4 aggregation WFs (using blocks: summarize_by_dept, summarize_by_faculty, etc.)
  Phase 4: 3 report WFs (using blocks: build_ranked_report)

Run:
  python university_200wf_wfblock.py --mode local
  python university_200wf_wfblock.py --mode cluster
"""


# ============================================================
# WfBlock: Reusable workflow building blocks for university domain
# ============================================================
class UniversityBlockDF(dataframe.DataFrame):
    """Reusable university analytics blocks.

    Encapsulates the repetitive patterns from university_200wf.py (200 WFs)
    into named methods. Compare with WorkflowBlockDF in omigo_crwd_wfs which
    does the same for security/telemetry operations.
    """
    def __init__(self, header_fields, data_fields):
        super().__init__(header_fields, data_fields)

    # --- Student filtering blocks ---

    def filter_active_students(self, min_gpa = 2.0):
        """Filter to active students with GPA >= threshold.

        Expects columns: status, gpa.
        Produces: same columns, filtered rows.

        Replaces (from 200wf):
            .eq_str("status", "active").ge_float("gpa", min_gpa)
        Used in: active_students, high_gpa_students, and 5+ other WFs.
        """
        return self \
            .eq_str("status", "active") \
            .ge_float("gpa", min_gpa)

    def filter_upper_level(self, min_level = 300.0):
        """Filter courses to upper-level (level >= threshold).

        Expects columns: level.
        Produces: same columns, filtered rows.

        Replaces (from 200wf):
            .ge_float("level", min_level)
        Used in: upper_courses, advanced_courses.
        """
        return self \
            .ge_float("level", min_level)

    def filter_passing(self):
        """Filter enrollments to passing grades (grade != F).

        Expects columns: grade.
        Produces: same columns, filtered rows.

        Replaces (from 200wf):
            .not_eq_str("grade", "F")
        Used in: passing_grades.
        """
        return self \
            .not_eq_str("grade", "F")

    def filter_tenured(self):
        """Filter faculty to non-adjunct (tenured/tenure-track).

        Expects columns: faculty_title.
        Produces: same columns, filtered rows.

        Replaces (from 200wf):
            .not_eq_str("faculty_title", "Adjunct")
        Used in: tenured_faculty.
        """
        return self \
            .not_eq_str("faculty_title", "Adjunct")

    # --- Enrichment/join blocks ---

    def enrich_with_dept(self, ctx):
        """Join with departments to add dept_name.

        Expects columns: dept_code.
        Produces: adds dept_name, building, budget from departments.

        Replaces (from 200wf):
            .inner_map_join(ctx.read_df("departments"), ["dept_code"])
        Used in: 15+ WFs including student_dept, faculty_dept, club_dept, etc.
        """
        return self \
            .inner_map_join(ctx.read_df("departments"), ["dept_code"])

    def enrich_with_course(self, ctx):
        """Join with courses to add course_name, credits, level.

        Expects columns: course_id.
        Produces: adds course_name, dept_code, credits, level from courses.

        Replaces (from 200wf):
            .inner_map_join(ctx.read_df("courses"), ["course_id"])
        Used in: course_enrollment, full_transcript, schedule_course, etc.
        """
        return self \
            .inner_map_join(ctx.read_df("courses"), ["course_id"])

    def enrich_with_student(self, ctx):
        """Join with students to add student_name, gpa, dept_code.

        Expects columns: student_id.
        Produces: adds student_name, gpa, dept_code, year, status from students.

        Replaces (from 200wf):
            .inner_map_join(ctx.read_df("students"), ["student_id"])
        Used in: member_student, intern_student, advisor_student, etc.
        """
        return self \
            .inner_map_join(ctx.read_df("students"), ["student_id"])

    def enrich_with_faculty(self, ctx):
        """Join with faculty to add faculty_name, dept_code.

        Expects columns: faculty_id.
        Produces: adds faculty_name, dept_code, faculty_title from faculty.

        Replaces (from 200wf):
            .inner_map_join(ctx.read_df("faculty"), ["faculty_id"])
        Used in: faculty_grants, faculty_pubs, advisor_faculty, etc.
        """
        return self \
            .inner_map_join(ctx.read_df("faculty"), ["faculty_id"])

    # --- Aggregation blocks ---

    def summarize_by_dept(self):
        """Aggregate by department with count and sort.

        Expects columns: dept_code, dept_name.
        Produces columns: dept_code, dept_name, group:count.

        Replaces (from 200wf):
            .group_count(["dept_code", "dept_name"]).sort("dept_code")
        Used in: 10+ WFs: dept_enrollment, grant_by_dept, pub_by_dept, etc.
        """
        return self \
            .group_count(["dept_code", "dept_name"]) \
            .sort("dept_code")

    def summarize_by_faculty(self):
        """Aggregate by faculty with count and sort.

        Expects columns: faculty_id, faculty_name.
        Produces columns: faculty_id, faculty_name, group:count.

        Replaces (from 200wf):
            .group_count(["faculty_id", "faculty_name"]).sort("faculty_id")
        Used in: pub_by_faculty, grant_by_faculty, advisor_by_faculty, etc.
        """
        return self \
            .group_count(["faculty_id", "faculty_name"]) \
            .sort("faculty_id")

    def summarize_by_course(self):
        """Aggregate by course with count and sort.

        Expects columns: course_id, course_name.
        Produces columns: course_id, course_name, group:count.

        Replaces (from 200wf):
            .group_count(["course_id", "course_name"]).sort("course_name")
        Used in: course_demand, high_gpa_by_course, senior_by_course, etc.
        """
        return self \
            .group_count(["course_id", "course_name"]) \
            .sort("course_name")

    # --- Report blocks ---

    def build_ranked_report(self, sort_col, rank_col = "rank"):
        """Sort by a column and add sequential rank number.

        Expects columns: sort_col must exist.
        Produces: adds rank_col with sequential numbering.

        Replaces (from 200wf):
            .sort(col).add_seq_num("rank")
        Used in: honor_roll, dept_report, faculty_output, executive reports.
        """
        return self \
            .sort(sort_col) \
            .add_seq_num(rank_col)


# ============================================================
# Layer 2: Hydra adapter — cluster execution support
# This wraps each block method as a ClusterMapOperation so it can
# run distributed on the Hydra cluster. Same pattern as
# HydraWorkflowBlockDF in omigo_crwd_wfs/wfblock_cluster_df.py.
# ============================================================
@hydra_class_for(UniversityBlockDF)
class HydraUniversityBlockDF(cluster_df.HydraBaseDF):
    def __init__(self, header_fields, data_fields, *args, **kwargs):
        super().__init__(header_fields, data_fields, *args, **kwargs)

    def filter_active_students(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.filter_active_students, self.requirements, *args, **kwargs))

    def filter_upper_level(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.filter_upper_level, self.requirements, *args, **kwargs))

    def filter_passing(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.filter_passing, self.requirements, *args, **kwargs))

    def filter_tenured(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.filter_tenured, self.requirements, *args, **kwargs))

    def enrich_with_dept(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.enrich_with_dept, self.requirements, *args, **kwargs))

    def enrich_with_course(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.enrich_with_course, self.requirements, *args, **kwargs))

    def enrich_with_student(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.enrich_with_student, self.requirements, *args, **kwargs))

    def enrich_with_faculty(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.enrich_with_faculty, self.requirements, *args, **kwargs))

    def summarize_by_dept(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.summarize_by_dept, self.requirements, *args, **kwargs))

    def summarize_by_faculty(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.summarize_by_faculty, self.requirements, *args, **kwargs))

    def summarize_by_course(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.summarize_by_course, self.requirements, *args, **kwargs))

    def build_ranked_report(self, *args, **kwargs):
        return cluster_df.HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(UniversityBlockDF.build_ranked_report, self.requirements, *args, **kwargs))


# ============================================================
# Main script
# ============================================================

# Command Line
parser = argparse.ArgumentParser(description = "SWF example: university analytics with WfBlock (200wf equivalent)")
parser.add_argument("--mode", choices = ["local", "cluster"], default = "local", help = "Execution mode: local (inmemory) or cluster")
args = parser.parse_args()

if (args.mode == "local"):
    os.environ.setdefault("HYDRA_PATH", "/tmp/hydra-v2-example")

# ============================================================
# 1. Initialize
# ============================================================
utils.info("=" * 70)
utils.info("1. Initializing (mode={})".format(args.mode))
utils.info("=" * 70)

if (args.mode == "cluster"):
    cluster_services_v2.init("university_200wf_wfblock")
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
# 5. Define WF blueprints using UniversityBlockDF
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("5. Defining WF blueprints (using UniversityBlockDF)")
utils.info("=" * 70)

# ------------------------------------------------------------------
# Phase 0 — Root WFs (8 WFs): inline synthetic data
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 0: Root datasets (8 WFs) ---")

# WF: students (root, 25 rows)
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

# WF: courses (root, 20 rows)
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

# WF: enrollments (root, 50 rows)
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
    {"student_id": "S003", "course_id": "C201", "grade": "B", "semester": "Fall2026"},
]).to_wf_spec()

utils.info("  enrollments:        {} op(s)  [root, 50 rows]".format(len(enrollments_bp.jobs_operations)))

# WF: faculty (root, 15 rows)
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

# WF: departments (root, 8 rows)
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

# WF: research_grants (root, 12 rows)
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

# WF: clubs (root, 12 rows)
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

# WF: internships (root, 18 rows)
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

# ------------------------------------------------------------------
# Phase 1 — Filters & lookups using blocks (5 WFs)
# In the 200wf original, this was 20 separate WFs with repeated inline patterns.
# With blocks, 5 WFs cover the same filtered datasets.
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 1: Filters & lookups via blocks (5 WFs) ---")

# WF: active_students — filter_active_students block with BVar threshold
# In 200wf, this exact pattern repeats in: active_students, high_gpa_students, senior_students, etc.
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status
active_students_bp = ctx.read_df("students") \
    .extend_class(UniversityBlockDF) \
    .filter_active_students(bctx.min_gpa.value) \
    .checkpoint("active_ckpt") \
    .to_wf_spec()

utils.info("  active_students:    {} op(s)  [filter_active_students block]".format(len(active_students_bp.jobs_operations)))

# WF: upper_courses — filter_upper_level block
# OUTPUT COLS: course_id, course_name, dept_code, credits, level
upper_courses_bp = ctx.read_df("courses") \
    .extend_class(UniversityBlockDF) \
    .filter_upper_level(300.0) \
    .checkpoint("upper_ckpt") \
    .to_wf_spec()

utils.info("  upper_courses:      {} op(s)  [filter_upper_level block]".format(len(upper_courses_bp.jobs_operations)))

# WF: passing_grades — filter_passing block
# OUTPUT COLS: student_id, course_id, grade, semester
passing_grades_bp = ctx.read_df("enrollments") \
    .extend_class(UniversityBlockDF) \
    .filter_passing() \
    .to_wf_spec()

utils.info("  passing_grades:     {} op(s)  [filter_passing block]".format(len(passing_grades_bp.jobs_operations)))

# WF: tenured_faculty — filter_tenured block
# OUTPUT COLS: faculty_id, faculty_name, dept_code, faculty_title
tenured_faculty_bp = ctx.read_df("faculty") \
    .extend_class(UniversityBlockDF) \
    .filter_tenured() \
    .to_wf_spec()

utils.info("  tenured_faculty:    {} op(s)  [filter_tenured block]".format(len(tenured_faculty_bp.jobs_operations)))

# WF: significant_grants — filter grants above threshold
# OUTPUT COLS: grant_id, faculty_id, grant_amount, grant_title, grant_year
significant_grants_bp = ctx.read_df("research_grants") \
    .ge_float("grant_amount", bctx.min_grant_amount.value) \
    .to_wf_spec()

utils.info("  significant_grants: {} op(s)  [filter grant_amount >= threshold]".format(len(significant_grants_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 2 — Joins using enrichment blocks (5 WFs)
# In the 200wf original, this was 25 WFs with repeated join patterns.
# With enrich_with_dept, enrich_with_course, enrich_with_faculty blocks,
# each WF is 2-3 lines instead of 4-6.
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 2: Joins via enrichment blocks (5 WFs) ---")

# WF: student_transcript — active students + passing grades + course info
# Without blocks: .inner_map_join(ctx.read_df("passing_grades"), [...]).inner_map_join(ctx.read_df("courses"), [...])
# With blocks: join passing_grades, then enrich_with_course block
# OUTPUT COLS: student_id, student_name, gpa, dept_code, year, status, course_id, grade, semester, course_name, dept_code:crs, credits, level
student_transcript_bp = ctx.read_df("active_students") \
    .inner_map_join(ctx.read_df("passing_grades"), ["student_id"]) \
    .extend_class(UniversityBlockDF) \
    .enrich_with_course(ctx) \
    .to_wf_spec()

utils.info("  student_transcript: {} op(s)  [join passing_grades + enrich_with_course block]".format(
    len(student_transcript_bp.jobs_operations)))

# WF: faculty_with_grants — tenured faculty enriched with grant info
# OUTPUT COLS: faculty_id, faculty_name, dept_code, faculty_title, grant_id, grant_amount, grant_title, grant_year
faculty_with_grants_bp = ctx.read_df("tenured_faculty") \
    .inner_map_join(ctx.read_df("significant_grants"), ["faculty_id"]) \
    .to_wf_spec()

utils.info("  faculty_with_grants: {} op(s)  [join tenured_faculty + significant_grants]".format(
    len(faculty_with_grants_bp.jobs_operations)))

# WF: intern_enriched — internships enriched with student info via block
# Without blocks: .inner_map_join(ctx.read_df("students"), ["student_id"])
# With blocks: .enrich_with_student(ctx)
# OUTPUT COLS: internship_id, student_id, company_name, intern_role, intern_semester, intern_stipend, student_name, gpa, dept_code, year, status
intern_enriched_bp = ctx.read_df("internships") \
    .extend_class(UniversityBlockDF) \
    .enrich_with_student(ctx) \
    .to_wf_spec()

utils.info("  intern_enriched:    {} op(s)  [enrich_with_student block]".format(
    len(intern_enriched_bp.jobs_operations)))

# WF: club_enriched — clubs enriched with dept info via block
# Without blocks: .inner_map_join(ctx.read_df("departments"), ["dept_code"])
# With blocks: .enrich_with_dept(ctx)
# OUTPUT COLS: club_id, club_name, dept_code, club_budget, dept_name, building, budget
club_enriched_bp = ctx.read_df("clubs") \
    .extend_class(UniversityBlockDF) \
    .enrich_with_dept(ctx) \
    .to_wf_spec()

utils.info("  club_enriched:      {} op(s)  [enrich_with_dept block]".format(
    len(club_enriched_bp.jobs_operations)))

# WF: faculty_dept — tenured faculty enriched with dept name
# OUTPUT COLS: faculty_id, faculty_name, dept_code, faculty_title, dept_name, building, budget
faculty_dept_bp = ctx.read_df("tenured_faculty") \
    .extend_class(UniversityBlockDF) \
    .enrich_with_dept(ctx) \
    .to_wf_spec()

utils.info("  faculty_dept:       {} op(s)  [enrich_with_dept block]".format(
    len(faculty_dept_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 3 — Aggregations using summarize blocks (4 WFs)
# In the 200wf original, this was 25 WFs with repeated group_count + sort.
# With summarize_by_dept, summarize_by_faculty, summarize_by_course blocks,
# each aggregation is a single method call.
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 3: Aggregations via summarize blocks (4 WFs) ---")

# WF: enrollment_by_dept — count students per department
# Without blocks: .group_count(["dept_code", "dept_name"]).sort("dept_code")
# With blocks: .summarize_by_dept()
# OUTPUT COLS: dept_code, dept_name, group:count
enrollment_by_dept_bp = ctx.read_df("student_transcript") \
    .select(["student_id", "dept_code"]) \
    .distinct() \
    .extend_class(UniversityBlockDF) \
    .enrich_with_dept(ctx) \
    .summarize_by_dept() \
    .to_wf_spec()

utils.info("  enrollment_by_dept: {} op(s)  [enrich_with_dept + summarize_by_dept blocks]".format(
    len(enrollment_by_dept_bp.jobs_operations)))

# WF: course_demand — count enrollments per course
# OUTPUT COLS: course_id, course_name, group:count
course_demand_bp = ctx.read_df("student_transcript") \
    .select(["course_id", "course_name"]) \
    .extend_class(UniversityBlockDF) \
    .summarize_by_course() \
    .to_wf_spec()

utils.info("  course_demand:      {} op(s)  [summarize_by_course block]".format(
    len(course_demand_bp.jobs_operations)))

# WF: grants_by_faculty — count grants per faculty member
# OUTPUT COLS: faculty_id, faculty_name, group:count
grants_by_faculty_bp = ctx.read_df("faculty_with_grants") \
    .extend_class(UniversityBlockDF) \
    .summarize_by_faculty() \
    .to_wf_spec()

utils.info("  grants_by_faculty:  {} op(s)  [summarize_by_faculty block]".format(
    len(grants_by_faculty_bp.jobs_operations)))

# WF: interns_by_dept — count interns per department
# OUTPUT COLS: dept_code, dept_name, group:count
interns_by_dept_bp = ctx.read_df("intern_enriched") \
    .select(["student_id", "dept_code"]) \
    .extend_class(UniversityBlockDF) \
    .enrich_with_dept(ctx) \
    .summarize_by_dept() \
    .to_wf_spec()

utils.info("  interns_by_dept:    {} op(s)  [enrich_with_dept + summarize_by_dept blocks]".format(
    len(interns_by_dept_bp.jobs_operations)))

# ------------------------------------------------------------------
# Phase 4 — Reports using build_ranked_report block (3 WFs)
# In the 200wf original, this was 10+ WFs with repeated sort + add_seq_num.
# With build_ranked_report block, ranking is a single call.
# ------------------------------------------------------------------
utils.info("")
utils.info("  --- Phase 4: Reports via build_ranked_report block (3 WFs) ---")

# WF: dept_enrollment_report — ranked departments by enrollment count
# Without blocks: .sort("group:count").add_seq_num("rank")
# With blocks: .build_ranked_report("group:count")
# OUTPUT COLS: dept_code, dept_name, group:count, rank
dept_enrollment_report_bp = ctx.read_df("enrollment_by_dept") \
    .extend_class(UniversityBlockDF) \
    .build_ranked_report("group:count") \
    .to_wf_spec()

utils.info("  dept_enrollment_report: {} op(s)  [build_ranked_report block]".format(
    len(dept_enrollment_report_bp.jobs_operations)))

# WF: course_demand_report — ranked courses by demand, param enrichment with semester
# OUTPUT COLS: course_id, course_name, group:count, ctx:semester, rank
course_demand_report_bp = ctx.read_df("course_demand", params = ["semester"], prefix = "ctx") \
    .extend_class(UniversityBlockDF) \
    .build_ranked_report("group:count") \
    .to_wf_spec()

utils.info("  course_demand_report: {} op(s)  [param enrichment + build_ranked_report block]".format(
    len(course_demand_report_bp.jobs_operations)))

# WF: faculty_grant_report — ranked faculty by grant count
# OUTPUT COLS: faculty_id, faculty_name, group:count, rank
faculty_grant_report_bp = ctx.read_df("grants_by_faculty") \
    .extend_class(UniversityBlockDF) \
    .build_ranked_report("group:count") \
    .to_wf_spec()

utils.info("  faculty_grant_report: {} op(s)  [build_ranked_report block]".format(
    len(faculty_grant_report_bp.jobs_operations)))

# ============================================================
# 6. Materialize SWF
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("6. Materializing SWF (mode={})".format(args.mode))
utils.info("=" * 70)

swf_id = (
    SWFBuilder(ctx, params = swf_params)
        # Phase 0: Roots
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
        .add_wf("research_grants", research_grants_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("clubs", clubs_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("internships", internships_bp,
                 is_live = True, bucket_interval = 5, duration = 30)
        # Phase 1: Filters
        .add_wf("active_students", active_students_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("upper_courses", upper_courses_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("passing_grades", passing_grades_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("tenured_faculty", tenured_faculty_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("significant_grants", significant_grants_bp,
                 is_live = True, bucket_interval = 10, duration = 30)
        # Phase 2: Joins
        .add_wf("student_transcript", student_transcript_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("faculty_with_grants", faculty_with_grants_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("intern_enriched", intern_enriched_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("club_enriched", club_enriched_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("faculty_dept", faculty_dept_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        # Phase 3: Aggregations
        .add_wf("enrollment_by_dept", enrollment_by_dept_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("course_demand", course_demand_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("grants_by_faculty", grants_by_faculty_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("interns_by_dept", interns_by_dept_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        # Phase 4: Reports
        .add_wf("dept_enrollment_report", dept_enrollment_report_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("course_demand_report", course_demand_report_bp,
                 is_live = True, bucket_interval = 20, duration = 30)
        .add_wf("faculty_grant_report", faculty_grant_report_bp,
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

if (args.mode == "local"):
    active_students_out = ctx.read_output("active_students")
    student_transcript_out = ctx.read_output("student_transcript")
    faculty_with_grants_out = ctx.read_output("faculty_with_grants")
    intern_enriched_out = ctx.read_output("intern_enriched")
    enrollment_by_dept_out = ctx.read_output("enrollment_by_dept")
    course_demand_out = ctx.read_output("course_demand")
    grants_by_faculty_out = ctx.read_output("grants_by_faculty")
    interns_by_dept_out = ctx.read_output("interns_by_dept")
    dept_enrollment_report_out = ctx.read_output("dept_enrollment_report")
    course_demand_report_out = ctx.read_output("course_demand_report")
    faculty_grant_report_out = ctx.read_output("faculty_grant_report")
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

    active_students_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "active_students")
    student_transcript_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "student_transcript")
    faculty_with_grants_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "faculty_with_grants")
    intern_enriched_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "intern_enriched")
    enrollment_by_dept_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "enrollment_by_dept")
    course_demand_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "course_demand")
    grants_by_faculty_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "grants_by_faculty")
    interns_by_dept_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "interns_by_dept")
    dept_enrollment_report_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "dept_enrollment_report")
    course_demand_report_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "course_demand_report")
    faculty_grant_report_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "faculty_grant_report")

utils.info("")
utils.info("  active_students:         {} rows, cols: {}".format(active_students_out.num_rows(), active_students_out.get_columns()))
utils.info("  student_transcript:      {} rows, cols: {}".format(student_transcript_out.num_rows(), student_transcript_out.get_columns()))
utils.info("  faculty_with_grants:     {} rows, cols: {}".format(faculty_with_grants_out.num_rows(), faculty_with_grants_out.get_columns()))
utils.info("  intern_enriched:         {} rows, cols: {}".format(intern_enriched_out.num_rows(), intern_enriched_out.get_columns()))
utils.info("  enrollment_by_dept:      {} rows, cols: {}".format(enrollment_by_dept_out.num_rows(), enrollment_by_dept_out.get_columns()))
utils.info("  course_demand:           {} rows, cols: {}".format(course_demand_out.num_rows(), course_demand_out.get_columns()))
utils.info("  grants_by_faculty:       {} rows, cols: {}".format(grants_by_faculty_out.num_rows(), grants_by_faculty_out.get_columns()))
utils.info("  interns_by_dept:         {} rows, cols: {}".format(interns_by_dept_out.num_rows(), interns_by_dept_out.get_columns()))
utils.info("  dept_enrollment_report:  {} rows, cols: {}".format(dept_enrollment_report_out.num_rows(), dept_enrollment_report_out.get_columns()))
utils.info("  course_demand_report:    {} rows, cols: {}".format(course_demand_report_out.num_rows(), course_demand_report_out.get_columns()))
utils.info("  faculty_grant_report:    {} rows, cols: {}".format(faculty_grant_report_out.num_rows(), faculty_grant_report_out.get_columns()))

# ============================================================
# Summary
# ============================================================
utils.info("")
utils.info("=" * 70)
utils.info("DONE - University analytics WfBlock example completed")
utils.info("=" * 70)
utils.info("")
utils.info("  Comparison with university_200wf.py:")
utils.info("    200wf:    200 WFs, 10 phases, ~1800 lines")
utils.info("    wfblock:   25 WFs,  5 phases, ~600 lines (same domain coverage)")
utils.info("")
utils.info("  Blocks used and their impact:")
utils.info("    filter_active_students()  — replaced 5+ inline filter chains")
utils.info("    filter_upper_level()      — replaced 2+ inline level filters")
utils.info("    filter_passing()          — replaced inline grade != F patterns")
utils.info("    filter_tenured()          — replaced inline title != Adjunct patterns")
utils.info("    enrich_with_dept(ctx)     — replaced 15+ inline dept joins")
utils.info("    enrich_with_course(ctx)   — replaced 10+ inline course joins")
utils.info("    enrich_with_student(ctx)  — replaced 8+ inline student joins")
utils.info("    enrich_with_faculty(ctx)  — replaced 5+ inline faculty joins")
utils.info("    summarize_by_dept()       — replaced 10+ inline group_count+sort")
utils.info("    summarize_by_faculty()    — replaced 5+ inline group_count+sort")
utils.info("    summarize_by_course()     — replaced 5+ inline group_count+sort")
utils.info("    build_ranked_report()     — replaced 10+ inline sort+add_seq_num")
utils.info("")
utils.info("  Key insight: WfBlock methods are composable —")
utils.info("    .extend_class(UniversityBlockDF).filter_active_students(3.5)")
utils.info("    .extend_class(UniversityBlockDF).enrich_with_dept(ctx).summarize_by_dept()")
utils.info("  Each call returns a DataFrame, so blocks chain with any other DF operation.")
