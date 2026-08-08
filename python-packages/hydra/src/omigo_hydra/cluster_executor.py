"""Local operation executor for SWFBuilder inmemory mode.

Applies serialized cluster operations to a DataFrame eagerly, without cluster submission.
Used by SWFBuilder.__materialize_local__() to run DAGs in-memory.

Execution logic mirrors cluster_protocol_v2.py execute_live_single_round (lines 2553-2582)
but without checkpoint writes, filesystem I/O, or message bus publishing.
"""

from omigo_core import utils
from omigo_hydra import cluster_common_v2, cluster_data, cluster_class_reflection


def __execute_operations_local__(xinput, jobs_operations, ctx):
    """Execute serialized operations on xinput eagerly. Returns result DataFrame.

    Args:
        xinput: input DataFrame
        jobs_operations: list of ClusterOperationJob (from WFBlueprint.jobs_operations)
        ctx: ClusterExecutorContext (for resolving HydraDF operands if needed)

    Each ClusterOperationJob has:
        - map_ops: list of ClusterMapOperation
        - reduce_op: ClusterReduceOperation or None
        - singleton_op: ClusterSingletonOperation or None
        - extend_class_op: ClusterExtendClassOperation or None
        - checkpoint_op: ClusterCheckpointOperation or None (skipped in local mode)
    """
    otsv = xinput

    for job_op in jobs_operations:
        # resolve extend_class if present (applies to all operations in this job segment)
        extend_class_obj = None
        if (job_op.extend_class_op is not None):
            extend_class_obj = cluster_common_v2.load_extend_class_obj(
                job_op.extend_class_op, otsv.get_header_fields(), otsv.get_data_fields())

        # execute map operations
        if (job_op.map_ops is not None):
            for op in job_op.map_ops:
                otsv = __apply_single_operation__(otsv, op, extend_class_obj, ctx)

        # execute reduce operation
        if (job_op.reduce_op is not None):
            otsv = __apply_single_operation__(otsv, job_op.reduce_op, extend_class_obj, ctx)

        # execute singleton operation
        if (job_op.singleton_op is not None):
            otsv = __apply_single_operation__(otsv, job_op.singleton_op, extend_class_obj, ctx)

        # checkpoint_op: skip in local mode (no filesystem writes)

    return otsv


def __apply_single_operation__(otsv, operation, extend_class_obj, ctx):
    """Apply a single ClusterOperation to a DataFrame. Returns new DataFrame.

    Mirrors cluster_protocol_v2.py lines 2560-2582:
    1. Deserialize args/kwargs from ClusterOperand wrappers to native Python
    2. Resolve function from fully-qualified name
    3. If extend_class_obj defined, call method on that object
    4. Otherwise, extend otsv to the defining class and call
    """
    # 1. deserialize args and kwargs
    args = cluster_data.load_native_objects(operation.args)
    kwargs = cluster_data.load_native_objects(operation.kwargs)

    # 2. resolve any HydraDF/WFBlueprint operands in args (for join, etc.)
    args = __resolve_hydra_operands__(args, ctx)

    # 3. determine which function to call
    if (extend_class_obj is not None):
        # method lives on the extend_class object
        func_base_name = operation.name.split(".")[-1]
        class_func = getattr(extend_class_obj, func_base_name)
    else:
        # lookup fully qualified function, extend otsv to defining class
        func = cluster_class_reflection.load_fully_qualified_func(operation.name)
        class_reference = cluster_class_reflection.get_class_that_defined_method(func)
        class_func = getattr(otsv.extend_class(class_reference), func.__name__)

    # 4. call the function
    otsv = class_func(*args, **kwargs)
    return otsv


def __resolve_hydra_operands__(args, ctx):
    """Resolve any WFBlueprint or proxy DataFrame operands in args.

    When a HydraDF is passed as an argument to an operation (e.g. join), it gets
    serialized as a WFBlueprint. At execution time, we execute it to get the actual DataFrame.

    When a proxy DataFrame (from ctx.read_df()) is passed as an argument, it gets
    serialized as a ClusterProxyDataFrame. At deserialization time, load_native_objects
    returns a dict with PROXY_DATAFRAME_MARKER_KEY. We resolve it via ctx.read_output().
    """
    from omigo_hydra.cluster_df import WFBlueprint
    resolved = []
    for arg in args:
        if (isinstance(arg, WFBlueprint)):
            # execute the nested WFBlueprint inline and use its result
            nested_result = __execute_operations_local__(arg.xinput, arg.jobs_operations, ctx)
            resolved.append(nested_result)
        elif (isinstance(arg, dict) and cluster_data.PROXY_DATAFRAME_MARKER_KEY in arg):
            # resolve proxy DataFrame from ctx store
            input_id = arg[cluster_data.PROXY_DATAFRAME_MARKER_KEY]
            if (not ctx.has_output(input_id)):
                raise Exception("__resolve_hydra_operands__: proxy input_id '{}' not in ctx store. Available: {}".format(
                    input_id, ctx.get_output_ids()))
            resolved.append(ctx.read_output(input_id))
        else:
            resolved.append(arg)
    return resolved
