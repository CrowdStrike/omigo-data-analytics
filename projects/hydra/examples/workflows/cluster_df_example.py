# Example: Using HydraDF for distributed execution on the iris dataset
#
# This demonstrates how HydraDF acts as a shadow copy of DataFrame.
# Operations are recorded (not executed) and submitted to the cluster
# when materialize() is called.
#
# Usage:
#   from omigo_hydra import cluster_df_example
#   cluster_df_example.run(ctx)
#
# where ctx is a hydra cluster context obtained from cluster_services_v2

from omigo_core import dataframe, utils
from omigo_hydra import hydra, cluster_df, cluster_services_v2

# -------------------------------------------------------------------------
# Step 1: Read the iris dataset locally to get header_fields and data_fields
# -------------------------------------------------------------------------
def run(ctx):
    # read the iris dataset to get its schema
    df = hydra.read("data/iris.tsv")
    header_fields = df.get_header_fields()
    data_fields = df.get_data_fields()

    # -------------------------------------------------------------------------
    # Step 2: Create HydraDF with the DataFrame schema and cluster context
    # -------------------------------------------------------------------------
    hdf = cluster_df.HydraDF(header_fields, data_fields)
    hdf.set_hydra_ctx(ctx)
    hdf.set_hydra_num_splits(4)

    # -------------------------------------------------------------------------
    # Step 3: Chain operations — these are recorded, not executed
    # -------------------------------------------------------------------------
    result = hdf \
        .eq_str("class", "Iris-setosa") \
        .select(["sepal_length", "sepal_width", "petal_length", "petal_width", "class"]) \
        .gt_float("sepal_length", 5.0) \
        .add_const("dataset", "iris") \
        .rename("class", "species") \
        .group_count("species")

    # -------------------------------------------------------------------------
    # Step 4: Inspect the recorded operations before submitting
    # -------------------------------------------------------------------------
    utils.info("Number of recorded operations: {}".format(len(result.operations)))
    for i in range(len(result.operations)):
        op = result.operations[i]
        utils.info("  Operation {}: {} (type: {})".format(i, op.name, op.task_type))

    # inspect job splitting
    jobs = result.__get_jobs_operations__()
    utils.info("Number of jobs: {}".format(len(jobs)))
    for i in range(len(jobs)):
        job = jobs[i]
        utils.info("  Job {}: map_ops={}, reduce_op={}, singleton_op={}".format(
            i, len(job.map_ops),
            job.reduce_op.name if job.reduce_op is not None else "None",
            job.singleton_op.name if job.singleton_op is not None else "None"))

    # -------------------------------------------------------------------------
    # Step 5: Submit the workflow to the cluster
    # -------------------------------------------------------------------------
    wf_id = result.materialize(
        input_ids = ["input_id1"],
        output_ids = ["output_id1"]
    )

    utils.info("Submitted workflow: {}".format(wf_id))

    # -------------------------------------------------------------------------
    # Step 6: Read the output (blocks until complete)
    # -------------------------------------------------------------------------
    output_df = cluster_services_v2.read_workflow_output(wf_id)
    output_df.show()

    return wf_id

# -------------------------------------------------------------------------
# Standalone dry-run: demonstrate operation recording without a cluster
# -------------------------------------------------------------------------
def dry_run():
    # read iris locally
    df = hydra.read("data/iris.tsv")
    header_fields = df.get_header_fields()
    data_fields = df.get_data_fields()

    # create HydraDF without context (no cluster submission)
    hdf = cluster_df.HydraDF(header_fields, data_fields)

    # chain operations — all recorded, nothing executed
    result = hdf \
        .eq_str("class", "Iris-setosa") \
        .select(["sepal_length", "sepal_width", "petal_length", "petal_width", "class"]) \
        .gt_float("sepal_length", 5.0) \
        .add_const("dataset", "iris") \
        .rename("class", "species") \
        .sort("sepal_length") \
        .group_count("species")

    # inspect
    utils.info("Recorded {} operations:".format(len(result.operations)))
    for i in range(len(result.operations)):
        op = result.operations[i]
        utils.info("  [{}] {} ({})".format(i, op.name, op.task_type))

    # inspect job splitting
    jobs = result.__get_jobs_operations__()
    utils.info("Split into {} jobs:".format(len(jobs)))
    for i in range(len(jobs)):
        job = jobs[i]
        utils.info("  Job {}: {} map ops, reduce={}, singleton={}".format(
            i,
            len(job.map_ops),
            job.reduce_op.name if job.reduce_op is not None else "None",
            job.singleton_op.name if job.singleton_op is not None else "None"))

    return result

if __name__ == "__main__":
    dry_run()
