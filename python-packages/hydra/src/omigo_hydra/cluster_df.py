# central place for all serialized version of the DataFrames
from omigo_core import utils, timefuncs
from omigo_core import dataframe
from omigo_hydra import cluster_common_v2
from omigo_hydra import cluster_class_reflection
from omigo_hydra.cluster_class_reflection import hydra_class_for
from omigo_hydra.cluster_common_v2 import ClusterCapabilities
from omigo_ext import jira_ext, logscale_ext
from omigo_hydra import message_bus
import sys

SINGLE_SPLIT = 1

class WFBlueprint:
    """WF business logic blueprint: operations + input data. No execution config."""
    def __init__(self, jobs_operations, xinput, num_splits=10):
        self.jobs_operations = jobs_operations
        self.xinput = xinput
        self.num_splits = num_splits

# this is a shadow copy of DataFrame. Means APIs exist but DataFrame is not a base class.
# TODO: None of the hydra class can return non DataFrame
# TODO: instead of taking Map Reduce for public methods, return the internal breakdown of omigo_core DataFrame apis
# its possible that the keys used for grouping are created later in some extension class
class HydraBaseDF:
    def __init__(self, header_fields, data_fields):
        self.header_fields = header_fields
        self.data_fields = data_fields
        self.ctx = None
        self.operations = []
        self.num_splits = 1
        self.requirements = []

    def set_hydra_ctx(self, ctx):
        self.ctx = ctx
        return self

    def set_hydra_operations(self, operations):
        self.operations = operations
        return self

    def set_hydra_num_splits(self, num_splits):
        self.num_splits = num_splits
        return self

    def set_hydra_requirements(self, requirements):
        self.requirements = requirements
        return self

    def __get_columns__(self):
        return self.header_fields

    def __copy_and_append_operations__(self, new_op):
        utils.debug("__copy_and_append_operations__: new_op: {}".format(new_op.name))
        # create new array
        new_operations = []
        for op in self.operations:
            new_operations.append(op)

        # append new operation
        new_operations.append(new_op)

        # return
        return new_operations

    # TODO: implement inline execution
    def collect(self, ctx = None, name = None, input_ids = None, output_ids = None, start_ts = None, use_full_data = None, tags = None):
        """Submit operation chain to cluster. Returns wf_id.

        All params use None defaults to force keyword-argument usage.
        """
        # validate
        if (ctx is None):
            raise Exception("HydraBaseDF.collect: ctx is required. Use collect(ctx = my_ctx, output_ids = [...])")
        if (isinstance(ctx, str)):
            raise Exception("HydraBaseDF.collect: ctx must be an ExecutorContext, not a string. "
                "Signature changed: first arg is now ctx, not name.")
        if (output_ids is None or len(output_ids) == 0):
            raise Exception("HydraBaseDF.collect: output_ids is required (non-empty list).")

        # apply defaults
        if (input_ids is None):
            input_ids = []
        if (tags is None):
            tags = {}
        if (name is None):
            name = ""

        # resolve start_ts as timestamp
        if (start_ts is not None):
            start_ts = timefuncs.datetime_to_utctimestamp(start_ts)

        # use self.ctx (bound at creation time) for actual submission
        if (self.ctx is not None):
            wf_id = self.ctx.execute_jobs(self.__get_df__(), self.__get_jobs_operations__(), name = name, input_ids = input_ids, output_ids = output_ids, start_ts = start_ts, use_full_data = use_full_data, tags = tags)
            return wf_id
        else:
            raise Exception("HydraBaseDF: collect(): self.ctx is None. Create HydraDF via ClusterExecutorContext.from_maps() or .read_df()")

    def materialize(self, ctx = None, input_ids = None, output_ids = None, start_ts = None, use_full_data = None, tags = None, name = None):
        """Submit operation chain to cluster. Returns wf_id.

        All parameters use None defaults to force callers to use keyword arguments.
        This prevents silent breakage from the old positional signature (name, input_ids, output_ids).
        """
        # validate
        if (ctx is None):
            raise Exception("HydraBaseDF.materialize: ctx is required. Use materialize(ctx = my_ctx, output_ids = [...])")
        if (isinstance(ctx, str)):
            raise Exception("HydraBaseDF.materialize: ctx must be an ExecutorContext, not a string. "
                "Signature changed: use materialize(ctx = my_ctx, output_ids = [...], name = '...')")
        if (output_ids is None or len(output_ids) == 0):
            raise Exception("HydraBaseDF.materialize: output_ids is required (non-empty list). "
                "No point materializing without storing output.")

        return self.collect(ctx = ctx, name = name, input_ids = input_ids, output_ids = output_ids,
            start_ts = start_ts, use_full_data = use_full_data, tags = tags)

    # materialize as a super workflow wrapping a single WF
    def materialize_as_swf(self, ctx = None, input_ids = None, output_ids = None, start_ts = None, use_full_data = None, tags = None, name = None):
        """Submit operation chain as single-WF SWF. Returns swf_id."""
        # validate
        if (ctx is None):
            raise Exception("HydraBaseDF.materialize_as_swf: ctx is required.")
        if (isinstance(ctx, str)):
            raise Exception("HydraBaseDF.materialize_as_swf: ctx must be an ExecutorContext, not a string.")
        if (output_ids is None or len(output_ids) == 0):
            raise Exception("HydraBaseDF.materialize_as_swf: output_ids is required (non-empty list).")

        # apply defaults
        if (input_ids is None):
            input_ids = []
        if (tags is None):
            tags = {}
        if (name is None):
            name = ""

        # resolve start_ts as timestamp
        if (start_ts is not None):
            start_ts = timefuncs.datetime_to_utctimestamp(start_ts)

        if (self.ctx is not None):
            swf_id = self.ctx.execute_jobs_as_swf(self.__get_df__(), self.__get_jobs_operations__(), name = name, input_ids = input_ids, output_ids = output_ids, start_ts = start_ts, use_full_data = use_full_data, tags = tags)
            return swf_id
        else:
            raise Exception("HydraBaseDF: materialize_as_swf(): self.ctx is None. Create HydraDF via ClusterExecutorContext.from_maps() or .read_df()")

    # build wf spec and input dataframe without submitting to cluster
    # When input_ids and output_ids are provided, returns (ClusterSpecWF, xdf) tuple (backward compatible).
    # When omitted, returns a WFBlueprint containing only business logic (for use with SWFBuilder).
    def to_wf_spec(self, name="", input_ids=[], output_ids=[], start_ts=None, use_full_data=False, tags={}):
        if (len(input_ids) > 0 and len(output_ids) > 0):
            # backward compatible: return (ClusterSpecWF, xdf) tuple
            # resolve start_ts as timestamp
            if (start_ts is not None):
                start_ts = timefuncs.datetime_to_utctimestamp(start_ts)

            # resolve context
            if (self.ctx is not None):
                jobs_operations = self.__get_jobs_operations__()
                wf_spec = self.ctx.build_wf_spec(jobs_operations, name = name, input_ids = input_ids, output_ids = output_ids, start_ts = start_ts, use_full_data = use_full_data, tags = tags)
                xdf = self.__get_df__()
                return wf_spec, xdf
            else:
                raise Exception("HydraBaseDF: to_wf_spec(): ctx is None")
        else:
            # new mode: return WFBlueprint (no execution config)
            return WFBlueprint(self.__get_jobs_operations__(), self.__get_df__(), self.num_splits)

    def persist(self, path):
        raise Exception("TBD")

    def checkpoint(self, name, overwrite=False):
        return self.__new_hydra_df__(cluster_common_v2.ClusterCheckpointOperation(name, overwrite=overwrite))

    def __get_df__(self):
        return dataframe.DataFrame(self.header_fields, self.data_fields)

    def get_operations(self):
        return self.operations

    def __get_jobs_operations__(self):
        # check if there are any reduce operations
        reduce_indexes = []
        for i in range(len(self.operations)):
            # check for operation type
            if (isinstance(self.operations[i], (cluster_common_v2.ClusterReduceOperation, cluster_common_v2.ClusterSingletonOperation, cluster_common_v2.ClusterCheckpointOperation))):
                reduce_indexes.append(i)

        # debug
        utils.debug("HydraBaseDF: __get_jobs_operations__: reduce operation indexes: {}".format(reduce_indexes))
        # for i in reduce_indexes:
        #     utils.debug("HydraBaseDF: __get_jobs_operations__: reduce operation index: {}: {}".format(i, self.operations[i].name))

        # create a temporary split
        job_mr_splits = []

        # if there are no reduce operations, then a single job with only maps
        if (len(reduce_indexes) == 0):
            map_ops = self.operations
            reduce_op = None
            singleton_op = None
            checkpoint_op = None
            job_mr_splits.append((map_ops, reduce_op, singleton_op, checkpoint_op))
        else:
            # iterate and add jobs
            for i in range(len(reduce_indexes)):
                # create placeholders
                map_ops = []
                reduce_op = None
                singleton_op = None
                checkpoint_op = None
                cluster_def_op = None

                # boundary condition
                index = reduce_indexes[i]
                reduce_op = self.operations[index] if (isinstance(self.operations[index], (cluster_common_v2.ClusterReduceOperation))) else None
                singleton_op = self.operations[index] if (isinstance(self.operations[index], (cluster_common_v2.ClusterSingletonOperation))) else None
                checkpoint_op = self.operations[index] if (isinstance(self.operations[index], (cluster_common_v2.ClusterCheckpointOperation))) else None

                # indexes
                if (i > 0):
                    prev_index = reduce_indexes[i-1]
                    map_ops = self.operations[prev_index+1:index]
                else:
                    map_ops = self.operations[0:index]

                # add to the job
                job_mr_splits.append((map_ops, reduce_op, singleton_op, checkpoint_op))

            # boundary condition if last sequence of operations was map only. TODO: dont use isinstance
            if (isinstance(self.operations[-1], (cluster_common_v2.ClusterReduceOperation, cluster_common_v2.ClusterSingletonOperation, cluster_common_v2.ClusterCheckpointOperation)) == False):
                map_ops = self.operations[reduce_indexes[-1]+1:]
                reduce_op = None
                singleton_op = None
                checkpoint_op = None
                job_mr_splits.append((map_ops, reduce_op, singleton_op, checkpoint_op))

        # create segments of map and reduce operations
        jobs_operations = []

        # job mr splits has map-reduce splits. Now split each further based on the ExtendClass
        for (map_ops, reduce_op, singleton_op, checkpoint_op) in job_mr_splits:
            # create indexes
            extend_class_indexes = []

            # check for None
            if (map_ops is not None and len(map_ops) > 0):
                # iterate
                for i in range(len(map_ops)):
                    # map_op
                    map_op = map_ops[i]

                    # check if this is extend class
                    if (isinstance(map_op, (cluster_common_v2.ClusterExtendClassOperation))):
                        extend_class_indexes.append(i)

                # debug
                utils.debug("HydraBaseDF: __get_jobs_operations__: extend_class_op indexes: {}".format(extend_class_indexes))
                # for i in extend_class_indexes:
                #     utils.debug("HydraBaseDF: __get_jobs_operations__: extend_class_op index: {}: {}".format(i, map_ops[i].name))

                # check if any indexes were found
                if (len(extend_class_indexes) > 0):
                    # iterate through indexes and split
                    for i in range(len(extend_class_indexes)):
                        cur_index = extend_class_indexes[i]

                        # the index is pointing to an extend class
                        class_def_op = map_ops[cur_index]

                        # first index is special
                        if (i == 0):
                            if (cur_index > 0):
                                map_ops2 = map_ops[0:cur_index]
                                reduce_op2 = None
                                singleton_op2 = None
                                jobs_operations.append(cluster_common_v2.ClusterOperationJob(map_ops2, reduce_op2, singleton_op2, None, checkpoint_op=None))

                        # check if it is the last split or before
                        if (i < len(extend_class_indexes) - 1):
                            # reduce_op is None for all but the last split
                            next_index = extend_class_indexes[i+1]
                            map_ops2 = map_ops[cur_index+1:next_index]
                            reduce_op2 = None
                            singleton_op2 = None
                            jobs_operations.append(cluster_common_v2.ClusterOperationJob(map_ops2, reduce_op2, singleton_op2, class_def_op, checkpoint_op=None))
                        else:
                            # last split gets the reduce and checkpoint
                            map_ops2 = map_ops[cur_index+1:]
                            reduce_op2 = reduce_op
                            singleton_op2 = singleton_op
                            jobs_operations.append(cluster_common_v2.ClusterOperationJob(map_ops2, reduce_op2, singleton_op2, class_def_op, checkpoint_op=checkpoint_op))
                else:
                    class_def_op = None
                    jobs_operations.append(cluster_common_v2.ClusterOperationJob(map_ops, reduce_op, singleton_op, class_def_op, checkpoint_op=checkpoint_op))
            else:
                class_def_op = None
                jobs_operations.append(cluster_common_v2.ClusterOperationJob(map_ops, reduce_op, singleton_op, class_def_op, checkpoint_op=checkpoint_op))

        # return
        return jobs_operations

# this is a shadow copy of DataFrame. Means APIs exist but DataFrame is not a base class.
@hydra_class_for(dataframe.DataFrame)
class HydraDF(HydraBaseDF):
    def __init__(self, header_fields, data_fields, ctx = None, operations = [], requirements = []):
        super().__init__(header_fields, data_fields)
        super().set_hydra_ctx(ctx)
        super().set_hydra_operations(operations)
        super().set_hydra_requirements(requirements)

    def __new_hydra_df__(self, new_op):
        new_hdf = HydraDF(self.header_fields, self.data_fields, self.ctx, self.__copy_and_append_operations__(new_op), self.requirements)
        new_hdf.set_hydra_num_splits(self.num_splits)
        return new_hdf

    # ==============================
    # MAP Operations
    # ==============================

    def to_string(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.to_string, self.requirements, *args, **kwargs))

    def get_content_as_string(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.get_content_as_string, self.requirements, *args, **kwargs))

    def serialize_as_string(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.serialize_as_string, self.requirements, *args, **kwargs))

    def validate(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.validate, self.requirements, *args, **kwargs))

    def has_col(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.has_col, self.requirements, *args, **kwargs))

    def select(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.select, self.requirements, *args, **kwargs))

    def select_and_add_empty_cols_if_missing(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.select_and_add_empty_cols_if_missing, self.requirements, *args, **kwargs))

    def not_select(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.not_select, self.requirements, *args, **kwargs))

    def select_cols_with_cond_exists(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.select_cols_with_cond_exists, self.requirements, *args, **kwargs))

    def select_rows_with_cols_cond_exists(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.select_rows_with_cols_cond_exists, self.requirements, *args, **kwargs))

    def select_rows_with_cond_exists(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.select_rows_with_cond_exists, self.requirements, *args, **kwargs))

    def select_rows_and_cols_with_cond_exists(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.select_rows_and_cols_with_cond_exists, self.requirements, *args, **kwargs))

    def select_any_cols_with_cond_exists_filter(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.select_any_cols_with_cond_exists_filter, self.requirements, *args, **kwargs))

    def select_any_cols_with_cond_exists_exclude_filter(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.select_any_cols_with_cond_exists_exclude_filter, self.requirements, *args, **kwargs))

    def select_all_cols_with_cond_exists_filter(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.select_all_cols_with_cond_exists_filter, self.requirements, *args, **kwargs))

    def select_all_cols_with_cond_exists_exclude_filter(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.select_all_cols_with_cond_exists_exclude_filter, self.requirements, *args, **kwargs))

    def values_not_in(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.values_not_in, self.requirements, *args, **kwargs))

    def values_in(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.values_in, self.requirements, *args, **kwargs))

    def not_match(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.not_match, self.requirements, *args, **kwargs))

    def not_regex_match(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.not_regex_match, self.requirements, *args, **kwargs))

    def match(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.match, self.requirements, *args, **kwargs))

    def regex_match(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.regex_match, self.requirements, *args, **kwargs))

    def not_eq(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.not_eq, self.requirements, *args, **kwargs))

    def eq(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.eq, self.requirements, *args, **kwargs))

    def eq_int(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.eq_int, self.requirements, *args, **kwargs))

    def eq_float(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.eq_float, self.requirements, *args, **kwargs))

    def eq_str(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.eq_str, self.requirements, *args, **kwargs))

    def not_eq_int(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.not_eq_int, self.requirements, *args, **kwargs))

    def not_eq_float(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.not_eq_float, self.requirements, *args, **kwargs))

    def not_eq_str(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.not_eq_str, self.requirements, *args, **kwargs))

    def is_nonzero(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.is_nonzero, self.requirements, *args, **kwargs))

    def is_nonzero_int(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.is_nonzero_int, self.requirements, *args, **kwargs))

    def is_nonzero_float(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.is_nonzero_float, self.requirements, *args, **kwargs))

    def lt_str(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.lt_str, self.requirements, *args, **kwargs))

    def le_str(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.le_str, self.requirements, *args, **kwargs))

    def gt_str(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.gt_str, self.requirements, *args, **kwargs))

    def ge_str(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.ge_str, self.requirements, *args, **kwargs))

    def gt(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.gt, self.requirements, *args, **kwargs))

    def gt_int(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.gt_int, self.requirements, *args, **kwargs))

    def gt_float(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.gt_float, self.requirements, *args, **kwargs))

    def ge(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.ge, self.requirements, *args, **kwargs))

    def ge_int(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.ge_int, self.requirements, *args, **kwargs))

    def ge_float(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.ge_float, self.requirements, *args, **kwargs))

    def lt(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.lt, self.requirements, *args, **kwargs))

    def lt_int(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.lt_int, self.requirements, *args, **kwargs))

    def lt_float(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.lt_float, self.requirements, *args, **kwargs))

    def le(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.le, self.requirements, *args, **kwargs))

    def le_int(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.le_int, self.requirements, *args, **kwargs))

    def le_float(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.le_float, self.requirements, *args, **kwargs))

    def startswith(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.startswith, self.requirements, *args, **kwargs))

    def not_startswith(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.not_startswith, self.requirements, *args, **kwargs))

    def endswith(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.endswith, self.requirements, *args, **kwargs))

    def not_endswith(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.not_endswith, self.requirements, *args, **kwargs))

    def is_empty_str(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.is_empty_str, self.requirements, *args, **kwargs))

    def is_nonempty_str(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.is_nonempty_str, self.requirements, *args, **kwargs))

    def replace_str_inline(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.replace_str_inline, self.requirements, *args, **kwargs))

    def transform(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.transform, self.requirements, *args, **kwargs))

    def transform_inline(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.transform_inline, self.requirements, *args, **kwargs))

    def transform_inline_log(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.transform_inline_log, self.requirements, *args, **kwargs))

    def transform_inline_log2(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.transform_inline_log2, self.requirements, *args, **kwargs))

    def transform_inline_log10(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.transform_inline_log10, self.requirements, *args, **kwargs))

    def transform_inline_log1p(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.transform_inline_log1p, self.requirements, *args, **kwargs))

    def transform_inline_log1p_base2(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.transform_inline_log1p_base2, self.requirements, *args, **kwargs))

    def transform_inline_log1p_base10(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.transform_inline_log1p_base10, self.requirements, *args, **kwargs))

    def rename(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.rename, self.requirements, *args, **kwargs))

    def filter(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.filter, self.requirements, *args, **kwargs))

    def exclude_filter(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.exclude_filter, self.requirements, *args, **kwargs))

    def ratio(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.ratio, self.requirements, *args, **kwargs))

    def ratio_const(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.ratio_const, self.requirements, *args, **kwargs))

    def apply_precision(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.apply_precision, self.requirements, *args, **kwargs))

    def skip(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.skip, self.requirements, *args, **kwargs))

    def skip_rows(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.skip_rows, self.requirements, *args, **kwargs))

    def last(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.last, self.requirements, *args, **kwargs))

    def take(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.take, self.requirements, *args, **kwargs))

    def add_seq_num(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.add_seq_num, self.requirements, *args, **kwargs))

    def add_const(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.add_const, self.requirements, *args, **kwargs))

    def add_const_if_missing(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.add_const_if_missing, self.requirements, *args, **kwargs))

    def add_empty_cols_if_missing(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.add_empty_cols_if_missing, self.requirements, *args, **kwargs))

    def add_row(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.add_row, self.requirements, *args, **kwargs))

    def add_map_as_row(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.add_map_as_row, self.requirements, *args, **kwargs))

    def assign_value(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.assign_value, self.requirements, *args, **kwargs))

    def concat_as_cols(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.concat_as_cols, self.requirements, *args, **kwargs))

    def add_col_prefix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.add_col_prefix, self.requirements, *args, **kwargs))

    def add_prefix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.add_prefix, self.requirements, *args, **kwargs))

    def add_suffix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.add_suffix, self.requirements, *args, **kwargs))

    def remove_suffix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.remove_suffix, self.requirements, *args, **kwargs))

    def rename_prefix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.rename_prefix, self.requirements, *args, **kwargs))

    def rename_suffix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.rename_suffix, self.requirements, *args, **kwargs))

    def remove_prefix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.remove_prefix, self.requirements, *args, **kwargs))

    def replace_prefix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.replace_prefix, self.requirements, *args, **kwargs))

    def replace_suffix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.replace_suffix, self.requirements, *args, **kwargs))

    def drop(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.drop, self.requirements, *args, **kwargs))

    def drop_cols(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.drop_cols, self.requirements, *args, **kwargs))

    def drop_cols_with_prefix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.drop_cols_with_prefix, self.requirements, *args, **kwargs))

    def drop_cols_with_suffix(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.drop_cols_with_suffix, self.requirements, *args, **kwargs))

    def drop_if_exists(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.drop_if_exists, self.requirements, *args, **kwargs))

    def drop_cols_if_exists(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.drop_cols_if_exists, self.requirements, *args, **kwargs))

    def drop_empty_cols(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.drop_empty_cols, self.requirements, *args, **kwargs))

    def drop_cols_with_prefix_except(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.drop_cols_with_prefix_except, self.requirements, *args, **kwargs))

    def drop_empty_rows(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.drop_empty_rows, self.requirements, *args, **kwargs))

    def cap_min_inline(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.cap_min_inline, self.requirements, *args, **kwargs))

    def cap_max_inline(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.cap_max_inline, self.requirements, *args, **kwargs))

    def cap_min(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.cap_min, self.requirements, *args, **kwargs))

    def cap_max(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.cap_max, self.requirements, *args, **kwargs))

    def copy(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.copy, self.requirements, *args, **kwargs))

    def sample(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.sample, self.requirements, *args, **kwargs))

    def sample_without_replacement(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.sample_without_replacement, self.requirements, *args, **kwargs))

    def sample_with_replacement(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.sample_with_replacement, self.requirements, *args, **kwargs))

    def sample_class(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.sample_class, self.requirements, *args, **kwargs))

    def url_encode_inline(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.url_encode_inline, self.requirements, *args, **kwargs))

    def url_decode_inline(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.url_decode_inline, self.requirements, *args, **kwargs))

    def url_encode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.url_encode, self.requirements, *args, **kwargs))

    def url_decode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.url_decode, self.requirements, *args, **kwargs))

    def resolve_url_encoded_cols(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.resolve_url_encoded_cols, self.requirements, *args, **kwargs))

    def resolve_url_encoded_list_cols(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.resolve_url_encoded_list_cols, self.requirements, *args, **kwargs))

    def resolve_all_url_encoded_cols(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.resolve_all_url_encoded_cols, self.requirements, *args, **kwargs))

    def base64_encode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.base64_encode, self.requirements, *args, **kwargs))

    def base64_decode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.base64_decode, self.requirements, *args, **kwargs))

    def base64_encode_inline(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.base64_encode_inline, self.requirements, *args, **kwargs))

    def base64_decode_inline(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.base64_decode_inline, self.requirements, *args, **kwargs))

    def to_int(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.to_int, self.requirements, *args, **kwargs))

    def to_numeric(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.to_numeric, self.requirements, *args, **kwargs))

    def split_batches(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.split_batches, self.requirements, *args, **kwargs))

    def generate_key_hash(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.generate_key_hash, self.requirements, *args, **kwargs))

    def cumulative_sum(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.cumulative_sum, self.requirements, *args, **kwargs))

    def replicate_rows(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.replicate_rows, self.requirements, *args, **kwargs))

    def explode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.explode, self.requirements, *args, **kwargs))

    def explode_json(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.explode_json, self.requirements, *args, **kwargs))

    def explode_json_v2(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.explode_json_v2, self.requirements, *args, **kwargs))

    def flatmap(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.flatmap, self.requirements, *args, **kwargs))

    def set_missing_values(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.set_missing_values, self.requirements, *args, **kwargs))

    def filter_json_by_xpath(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.filter_json_by_xpath, self.requirements, *args, **kwargs))

    def reorder(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.reorder, self.requirements, *args, **kwargs))

    def reorder_reverse(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.reorder_reverse, self.requirements, *args, **kwargs))

    def reverse_reorder(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.reverse_reorder, self.requirements, *args, **kwargs))

    def noop(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.noop, self.requirements, *args, **kwargs))

    def custom_func(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.custom_func, self.requirements, *args, **kwargs))

    def write(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.write, self.requirements, *args, **kwargs))

    def warn(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.warn, self.requirements, *args, **kwargs))

    def warn_once(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.warn_once, self.requirements, *args, **kwargs))

    def warn_if_limit_reached(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.warn_if_limit_reached, self.requirements, *args, **kwargs))

    def sleep(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.sleep, self.requirements, *args, **kwargs))

    def split(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.split, self.requirements, *args, **kwargs))

    def split_str(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.split_str, self.requirements, *args, **kwargs))

    def resolve_template_col(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.resolve_template_col, self.requirements, *args, **kwargs))

    def resolve_template_col_inline(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.resolve_template_col_inline, self.requirements, *args, **kwargs))

    def get_header_fields(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.get_header_fields, self.requirements, *args, **kwargs))

    def get_data_fields(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.get_data_fields, self.requirements, *args, **kwargs))

    def get_header_map(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.get_header_map, self.requirements, *args, **kwargs))

    def num_rows(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.num_rows, self.requirements, *args, **kwargs))

    def num_cols(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.num_cols, self.requirements, *args, **kwargs))

    def get_size_in_bytes(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.get_size_in_bytes, self.requirements, *args, **kwargs))

    def size_in_bytes(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.size_in_bytes, self.requirements, *args, **kwargs))

    def size_in_mb(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.size_in_mb, self.requirements, *args, **kwargs))

    def size_in_gb(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.size_in_gb, self.requirements, *args, **kwargs))

    def get_columns(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.get_columns, self.requirements, *args, **kwargs))

    def get_column(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.get_column, self.requirements, *args, **kwargs))

    def columns(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.columns, self.requirements, *args, **kwargs))

    def get_column_index(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.get_column_index, self.requirements, *args, **kwargs))

    def get_col_index(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.get_col_index, self.requirements, *args, **kwargs))

    def is_empty(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.is_empty, self.requirements, *args, **kwargs))

    def has_empty_header(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.has_empty_header, self.requirements, *args, **kwargs))

    def get_hash(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.get_hash, self.requirements, *args, **kwargs))

    def export_to_maps(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.export_to_maps, self.requirements, *args, **kwargs))

    def to_maps(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.to_maps, self.requirements, *args, **kwargs))

    def to_maps_with_empty_data(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.to_maps_with_empty_data, self.requirements, *args, **kwargs))

    def first_value_with_default_if_missing(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.first_value_with_default_if_missing, self.requirements, *args, **kwargs))

    def array_matches(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.array_matches, self.requirements, *args, **kwargs))

    def array_value_exists(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.array_value_exists, self.requirements, *args, **kwargs))

    def array_value_find(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.array_value_find, self.requirements, *args, **kwargs))

    def nested_array_matches(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.nested_array_matches, self.requirements, *args, **kwargs))

    def nested_array_value_exists(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.nested_array_value_exists, self.requirements, *args, **kwargs))

    def nested_array_value_find(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.nested_array_value_find, self.requirements, *args, **kwargs))

    def enable_info_mode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.enable_info_mode, self.requirements, *args, **kwargs))

    def disable_info_mode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.disable_info_mode, self.requirements, *args, **kwargs))

    def enable_debug_mode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.enable_debug_mode, self.requirements, *args, **kwargs))

    def disable_debug_mode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.disable_debug_mode, self.requirements, *args, **kwargs))

    def enable_trace_mode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.enable_trace_mode, self.requirements, *args, **kwargs))

    def disable_trace_mode(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterMapOperation(dataframe.DataFrame.disable_trace_mode, self.requirements, *args, **kwargs))

    # ==============================
    # REDUCE Operations
    # ==============================

    def group_count(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.group_count, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def group_by_key(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.group_by_key, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def arg_min(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.arg_min, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def arg_max(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.arg_max, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def aggregate(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.aggregate, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def distinct(self, *args, **kwargs):
        grouping_cols = self.__get_columns__()
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.distinct, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def distinct_cols(self, *args, **kwargs):
        grouping_cols = self.__get_columns__()
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.distinct_cols, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def sample_group_by_col_value(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.sample_group_by_col_value, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def sample_group_by_max_uniq_values_exact(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.sample_group_by_max_uniq_values_exact, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def sample_group_by_max_uniq_values_approx(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.sample_group_by_max_uniq_values_approx, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def sample_group_by_max_uniq_values(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.sample_group_by_max_uniq_values, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def sample_group_by_max_uniq_values_per_class(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.sample_group_by_max_uniq_values_per_class, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def sample_group_by_key(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.sample_group_by_key, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def sample_column_by_max_uniq_values(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.sample_column_by_max_uniq_values, self.requirements, *args, **kwargs))

    def sample_class_by_min_class_count(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.sample_class_by_min_class_count, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def sample_class_by_max_values(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.sample_class_by_max_values, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def sample_group_by_topk(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.sample_group_by_topk, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    def sample_group_by_topk_if_reached_limit(self, *args, **kwargs):
        limit = args[0]
        grouping_cols = args[1]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.sample_group_by_topk_if_reached_limit, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    # ==============================
    # SINGLETON Operations
    # ==============================

    def sort(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.sort, self.requirements, *args, **kwargs))

    def reverse_sort(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.reverse_sort, self.requirements, *args, **kwargs))

    def numerical_sort(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.numerical_sort, self.requirements, *args, **kwargs))

    def reverse_numerical_sort(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.reverse_numerical_sort, self.requirements, *args, **kwargs))

    def show(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.show, self.requirements, *args, **kwargs))

    def show_sample(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.show_sample, self.requirements, *args, **kwargs))

    def show_transpose(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.show_transpose, self.requirements, *args, **kwargs))

    def show_transpose_non_empty(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.show_transpose_non_empty, self.requirements, *args, **kwargs))

    def show_transpose_sort_cols(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.show_transpose_sort_cols, self.requirements, *args, **kwargs))

    def show_transpose_non_empty_sort_cols(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.show_transpose_non_empty_sort_cols, self.requirements, *args, **kwargs))

    def show_group_count(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.show_group_count, self.requirements, *args, **kwargs))

    def show_select_func(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.show_select_func, self.requirements, *args, **kwargs))

    def show_custom_func(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.show_custom_func, self.requirements, *args, **kwargs))

    def show_transpose_custom_func(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.show_transpose_custom_func, self.requirements, *args, **kwargs))

    def print(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.print, self.requirements, *args, **kwargs))

    def print_stats(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.print_stats, self.requirements, *args, **kwargs))

    def get_max_size_cols_stats(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.get_max_size_cols_stats, self.requirements, *args, **kwargs))

    def col_as_array(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.col_as_array, self.requirements, *args, **kwargs))

    def col_as_float_array(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.col_as_float_array, self.requirements, *args, **kwargs))

    def col_as_int_array(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.col_as_int_array, self.requirements, *args, **kwargs))

    def col_as_array_uniq(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.col_as_array_uniq, self.requirements, *args, **kwargs))

    def col_as_array_uniq_non_empty(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.col_as_array_uniq_non_empty, self.requirements, *args, **kwargs))

    def cols_as_map(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.cols_as_map, self.requirements, *args, **kwargs))

    def union(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.union, self.requirements, *args, **kwargs))

    def merge_union(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.merge_union, self.requirements, *args, **kwargs))

    def merge_intersect(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.merge_intersect, self.requirements, *args, **kwargs))

    def difference(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.difference, self.requirements, *args, **kwargs))

    def left_join(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.left_join, self.requirements, *args, **kwargs))

    def right_join(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.right_join, self.requirements, *args, **kwargs))

    def inner_join(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.inner_join, self.requirements, *args, **kwargs))

    def outer_join(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.outer_join, self.requirements, *args, **kwargs))

    def join(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.join, self.requirements, *args, **kwargs))

    def natural_join(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.natural_join, self.requirements, *args, **kwargs))

    def inner_map_join(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.inner_map_join, self.requirements, *args, **kwargs))

    def left_map_join(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.left_map_join, self.requirements, *args, **kwargs))

    def transpose(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.transpose, self.requirements, *args, **kwargs))

    def reverse_transpose(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.reverse_transpose, self.requirements, *args, **kwargs))

    def to_tuples(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.to_tuples, self.requirements, *args, **kwargs))

    def to_df(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.to_df, self.requirements, *args, **kwargs))

    def to_simple_df(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.to_simple_df, self.requirements, *args, **kwargs))

    def to_pandas_df(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.to_pandas_df, self.requirements, *args, **kwargs))

    def export_to_df(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.export_to_df, self.requirements, *args, **kwargs))

    def to_json_records(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.to_json_records, self.requirements, *args, **kwargs))

    def to_csv_text(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.to_csv_text, self.requirements, *args, **kwargs))

    def sample_rows(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.sample_rows, self.requirements, *args, **kwargs))

    def sample_n(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.sample_n, self.requirements, *args, **kwargs))

    def sample_n_with_warn(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.sample_n_with_warn, self.requirements, *args, **kwargs))

    def sample_n_with_replacement(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.sample_n_with_replacement, self.requirements, *args, **kwargs))

    def sample_n_without_replacement(self, *args, **kwargs):
        return self.__new_hydra_df__(cluster_common_v2.ClusterSingletonOperation(dataframe.DataFrame.sample_n_without_replacement, self.requirements, *args, **kwargs))

    def window_aggregate(self, *args, **kwargs):
        # create operation
        select_cols = kwargs["select_cols"] if ("select_cols" in kwargs.keys() and kwargs["select_cols"] is not None) else []

        # create grouping cols
        grouping_cols = []
        for c in select_cols:
            grouping_cols.append(c)
        grouping_cols.append(args[0])

        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, dataframe.DataFrame.window_aggregate, self.requirements, *args, **kwargs)
        return self.__new_hydra_df__(op)

    # ==============================
    # SPECIAL: extend_class
    # ==============================

    def extend_class(self, newclass, *args, **kwargs):
        # Legacy no-op: ctx.from_maps() now returns HydraDF directly, so extend_class(HydraDF) is unnecessary
        if (newclass is HydraDF or newclass is HydraBaseDF):
            utils.warn_once("HydraDF.extend_class(HydraDF): no-op. ctx.from_maps() already returns HydraDF. Remove this call.")
            return self

        # find the corresponding hydra version of the class
        hydra_class_ref = cluster_class_reflection.get_hydra_class(newclass, [sys.modules[__name__]])

        # check for None
        if (hydra_class_ref is None):
            raise Exception("HydraDF: extend_class: not able to find hydra version for class: {}".format(newclass.__name__))

        # add the parameters
        hydra_class_obj = hydra_class_ref(self.header_fields, self.data_fields)
        hydra_class_obj.set_hydra_ctx(self.ctx)
        hydra_class_obj.set_hydra_operations(self.__copy_and_append_operations__(cluster_common_v2.ClusterExtendClassOperation(newclass, hydra_class_obj.requirements, *args, **kwargs)))

        # return
        return hydra_class_obj

    # ==============================
    # NOT IMPLEMENTED
    # ==============================

    def extend_external_class(self, *args, **kwargs):
        raise Exception("Not implemented")

    def to_clipboard(self, *args, **kwargs):
        raise Exception("Not implemented")

class HydraHelper:
    def new_hydra_df(hydra_base, new_op):
        new_hdf = HydraDF(hydra_base.header_fields, hydra_base.data_fields, ctx = hydra_base.ctx, operations = hydra_base.__copy_and_append_operations__(new_op), requirements = hydra_base.requirements)
        new_hdf.set_hydra_num_splits(hydra_base.num_splits)
        return new_hdf

@hydra_class_for(jira_ext.JiraDF)
class HydraJiraDF(HydraBaseDF):
    def __init__(self, header_fields, data_fields, *args, **kwargs):
        super().__init__(header_fields, data_fields, *args, **kwargs)
        super().set_hydra_requirements([ClusterCapabilities.JIRA])

    def search_issues(self, *args, **kwargs):
        return HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(jira_ext.JiraDF.search_issues, self.requirements, *args, **kwargs))

@hydra_class_for(logscale_ext.LogScaleDF)
class HydraLogScaleDF(HydraBaseDF):
    def __init__(self, header_fields, data_fields, *args, **kwargs):
        super().__init__(header_fields, data_fields, *args, **kwargs)
        super().set_hydra_requirements([ClusterCapabilities.LOGSCALE])

    def get_events(self, *args, **kwargs):
        return HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(logscale_ext.LogScaleDF.get_events, self.requirements, *args, **kwargs))

    def get_events_parsed(self, *args, **kwargs):
        return HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(logscale_ext.LogScaleDF.get_events_parsed, self.requirements, *args, **kwargs))

@hydra_class_for(message_bus.MessageBusRollupDF)
class HydraMessageBusRollupDF(HydraBaseDF):
    def __init__(self, header_fields, data_fields, *args, **kwargs):
        super().__init__(header_fields, data_fields, *args, **kwargs)

    def rollup_cascade(self, *args, **kwargs):
        return HydraHelper.new_hydra_df(self, cluster_common_v2.ClusterMapOperation(message_bus.MessageBusRollupDF.rollup_cascade, self.requirements, *args, **kwargs))
