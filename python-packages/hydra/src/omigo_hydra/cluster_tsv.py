# central place for all serialized version of the tsvs
from omigo_core import tsv, utils, timefuncs
from omigo_hydra import cluster_common_v2
from omigo_hydra import cluster_class_reflection
from omigo_hydra.cluster_common_v2 import ClusterCapabilities
import sys
from omigo_ext.splunk_ext import SplunkTSV
from omigo_hydra.cluster_shell_ext import SparkJobShellExecutorTSV
from omigo_core.tsv import TSV

SINGLE_SPLIT = 1
# awk command
# awk -F '(' '{print $1}'|awk -F' ' '{print "    def "$2"(self, *args, **kwargs):\n        return HydraTSV(self.header, self.data, super().ctx, super().__copy_and_append_operations__(cluster_common_v2.ClusterMapOperation(TSV."$2", self.requirements, *args, **kwargs))\n"}'|pbcopy
# TODO: None of the hydra class can return non TSV
# TODO: instead of taking Map Reduce for public methods, return the internal breakdown of omigo_core TSV apis
# its possible that the keys used for grouping are created later in some extension class
class HydraBaseTSV:
    def __init__(self, header, data):
        self.header = header
        self.data = data
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
        return self.header.split("\t")

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
    def collect(self, input_ids, output_ids, start_ts = None, use_full_data = False):
        # resolve start_ts as timestamp
        if (start_ts is not None):
            start_ts = timefuncs.datetime_to_utctimestamp(start_ts)

        # resolve context
        if (self.ctx is not None):
            wf_id = self.ctx.execute_jobs(self.__get_tsv__(), self.__get_jobs_operations__(), input_ids, output_ids, start_ts = start_ts, use_full_data = use_full_data)
            return wf_id
        else:
            raise Exception("HydraBaseTSV: collect(): ctx is None and in-memory execution is yet to be implemented")

    # TODO: single output
    def materialize(self, input_ids, output_ids, start_ts = None, use_full_data = False):
        return self.collect(input_ids, output_ids, start_ts = start_ts, use_full_data = use_full_data)

    def persist(self, path):
        raise Exception("TBD") 
  
    def __get_tsv__(self):
        return tsv.TSV(self.header, self.data)

    def get_operations(self):
        return self.operations

    def __get_jobs_operations__(self):
        # check if there are any reduce operations
        reduce_indexes = []
        for i in range(len(self.operations)):
            # check for operation type
            if (isinstance(self.operations[i], (cluster_common_v2.ClusterReduceOperation, cluster_common_v2.ClusterSingletonOperation))):
                reduce_indexes.append(i)

        # debug
        utils.debug("HydraBaseTSV: __get_jobs_operations__: reduce operation indexes: {}".format(reduce_indexes))
        # for i in reduce_indexes:
        #     utils.debug("HydraBaseTSV: __get_jobs_operations__: reduce operation index: {}: {}".format(i, self.operations[i].name))

        # create a temporary split
        job_mr_splits = []

        # if there are no reduce operations, then a single job with only maps
        if (len(reduce_indexes) == 0):
            map_ops = self.operations
            reduce_op = None
            singleton_op = None
            job_mr_splits.append((map_ops, reduce_op, singleton_op))
        else:
            # iterate and add jobs
            for i in range(len(reduce_indexes)):
                # create placeholders
                map_ops = [] 
                reduce_op = None
                singleton_op = None
                cluster_def_op = None 

                # boundary condition
                index = reduce_indexes[i]
                reduce_op = self.operations[index] if (isinstance(self.operations[index], (cluster_common_v2.ClusterReduceOperation))) else None
                singleton_op = self.operations[index] if (isinstance(self.operations[index], (cluster_common_v2.ClusterSingletonOperation))) else None

                # indexes
                if (i > 0):
                    prev_index = reduce_indexes[i-1] 
                    map_ops = self.operations[prev_index+1:index]
                else:
                    map_ops = self.operations[0:index]

                # add to the job
                job_mr_splits.append((map_ops, reduce_op, singleton_op))

            # boundary condition if last sequence of operations was map only. TODO: dont use isinstance
            if (isinstance(self.operations[-1], (cluster_common_v2.ClusterReduceOperation), cluster_common_v2.ClusterSingletonOperation) == False):
                map_ops = self.operations[reduce_indexes[-1]+1:]
                reduce_op = None
                singleton_op = None
                job_mr_splits.append((map_ops, reduce_op, singleton_op))

        # create segments of map and reduce operations
        jobs_operations = []

        # job mr splits has map-reduce splits. Now split each further based on the ExtendClass
        for (map_ops, reduce_op, singleton_op) in job_mr_splits:
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
                utils.debug("HydraBaseTSV: __get_jobs_operations__: extend_class_op indexes: {}".format(extend_class_indexes))
                # for i in extend_class_indexes:
                #     utils.debug("HydraBaseTSV: __get_jobs_operations__: extend_class_op index: {}: {}".format(i, map_ops[i].name))

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
                                jobs_operations.append(cluster_common_v2.ClusterOperationJob(map_ops2, reduce_op2, singleton_op2, None))
                            
                        # check if it is the last split or before
                        if (i < len(extend_class_indexes) - 1):
                            # reduce_op is None for all but the last split
                            next_index = extend_class_indexes[i+1]
                            map_ops2 = map_ops[cur_index+1:next_index]
                            reduce_op2 = None
                            singleton_op2 = None
                            jobs_operations.append(cluster_common_v2.ClusterOperationJob(map_ops2, reduce_op2, singleton_op2, class_def_op))
                        else:
                            # last split gets the reduce
                            map_ops2 = map_ops[cur_index+1:]
                            reduce_op2 = reduce_op
                            singleton_op2 = singleton_op
                            jobs_operations.append(cluster_common_v2.ClusterOperationJob(map_ops2, reduce_op2, singleton_op2, class_def_op))
                else:
                    class_def_op = None
                    jobs_operations.append(cluster_common_v2.ClusterOperationJob(map_ops, reduce_op, singleton_op, class_def_op))
            else:
                class_def_op = None
                jobs_operations.append(cluster_common_v2.ClusterOperationJob(map_ops, reduce_op, singleton_op, class_def_op))

        # return
        return jobs_operations

# this is a shadow copy of TSV. Means APIs exist but TSV is not a base class.
class HydraTSV(HydraBaseTSV):
    def __init__(self, header, data, ctx = None, operations = [], requirements = []):
        super().__init__(header, data)
        super().set_hydra_ctx(ctx)
        super().set_hydra_operations(operations)
        super().set_hydra_requirements(requirements)

    def to_string(self, *args, **kwargs):
        raise Exception("Not implemented")

    def __new_hydra_tsv__(self, new_op):
        return HydraTSV(self.header, self.data, self.ctx, self.__copy_and_append_operations__(new_op))

    def to_string(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.to_string, self.requirements, *args, **kwargs))

    def get_content_as_string(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_content_as_string, self.requirements, *args, **kwargs))

    def validate(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.validate, self.requirements, *args, **kwargs))

    def has_col(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.has_col, self.requirements, *args, **kwargs))

    def select(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.select, self.requirements, *args, **kwargs))

    def values_not_in(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.values_not_in, self.requirements, *args, **kwargs))

    def values_in(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.values_in, self.requirements, *args, **kwargs))

    def not_match(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.not_match, self.requirements, *args, **kwargs))

    def not_regex_match(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.not_regex_match, self.requirements, *args, **kwargs))

    def match(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.match, self.requirements, *args, **kwargs))

    def regex_match(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.regex_match, self.requirements, *args, **kwargs))

    def not_eq(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.not_eq, self.requirements, *args, **kwargs))

    def eq(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.eq, self.requirements, *args, **kwargs))

    def eq_int(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.eq_int, self.requirements, *args, **kwargs))

    def eq_float(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.eq_float, self.requirements, *args, **kwargs))

    def eq_str(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.eq_str, self.requirements, *args, **kwargs))

    def not_eq_int(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.not_eq_int, self.requirements, *args, **kwargs))

    def not_eq_float(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.not_eq_float, self.requirements, *args, **kwargs))

    def not_eq_str(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.not_eq_str, self.requirements, *args, **kwargs))

    def is_nonzero(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.is_nonzero, self.requirements, *args, **kwargs))

    def is_nonzero_int(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.is_nonzero_int, self.requirements, *args, **kwargs))

    def is_nonzero_float(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.is_nonzero_float, self.requirements, *args, **kwargs))

    def lt_str(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.lt_str, self.requirements, *args, **kwargs))

    def le_str(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.le_str, self.requirements, *args, **kwargs))

    def gt_str(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.gt_str, self.requirements, *args, **kwargs))

    def ge_str(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.ge_str, self.requirements, *args, **kwargs))

    def gt(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.gt, self.requirements, *args, **kwargs))

    def gt_int(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.gt_int, self.requirements, *args, **kwargs))

    def gt_float(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.gt_float, self.requirements, *args, **kwargs))

    def ge(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.ge, self.requirements, *args, **kwargs))

    def ge_int(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.ge_int, self.requirements, *args, **kwargs))

    def ge_float(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.ge_float, self.requirements, *args, **kwargs))

    def lt(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.lt, self.requirements, *args, **kwargs))

    def lt_int(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.lt_int, self.requirements, *args, **kwargs))

    def lt_float(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.lt_float, self.requirements, *args, **kwargs))

    def le(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.le, self.requirements, *args, **kwargs))

    def le_int(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.le_int, self.requirements, *args, **kwargs))

    def le_float(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.le_float, self.requirements, *args, **kwargs))

    def startswith(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.startswith, self.requirements, *args, **kwargs))

    def not_startswith(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.not_startswith, self.requirements, *args, **kwargs))

    def endswith(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.endswith, self.requirements, *args, **kwargs))

    def not_endswith(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.not_endswith, self.requirements, *args, **kwargs))

    def is_empty_str(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.is_empty_str, self.requirements, *args, **kwargs))

    def is_nonempty_str(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.is_nonempty_str, self.requirements, *args, **kwargs))

    def replace_str_inline(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.replace_str_inline, self.requirements, *args, **kwargs))

    def group_count(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.group_count, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def ratio(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.ratio, self.requirements, *args, **kwargs))

    def ratio_const(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.ratio_const, self.requirements, *args, **kwargs))

    def apply_precision(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.apply_precision, self.requirements, *args, **kwargs))

    def skip(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.skip, self.requirements, *args, **kwargs))

    def skip_rows(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.skip_rows, self.requirements, *args, **kwargs))

    def last(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.last, self.requirements, *args, **kwargs))

    def take(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.take, self.requirements, *args, **kwargs))

    def distinct(self, *args, **kwargs):
        grouping_cols = self.__get_columns__()
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.distinct, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def distinct_cols(self, *args, **kwargs):
        grouping_cols = self.__get_columns__()
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.distinct_cols, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def drop(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.drop, self.requirements, *args, **kwargs))

    def drop_cols(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.drop_cols, self.requirements, *args, **kwargs))

    def drop_cols_with_prefix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.drop_cols_with_prefix, self.requirements, *args, **kwargs))

    def drop_cols_with_suffix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.drop_cols_with_suffix, self.requirements, *args, **kwargs))

    def drop_if_exists(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.drop_if_exists, self.requirements, *args, **kwargs))

    def drop_cols_if_exists(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.drop_cols_if_exists, self.requirements, *args, **kwargs))

    def drop_empty_cols(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.drop_empty_cols, self.requirements, *args, **kwargs))

    def drop_empty_rows(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.drop_empty_rows, self.requirements, *args, **kwargs))

    def window_aggregate(self, *args, **kwargs):
        # create operation
        select_cols = kwargs["select_cols"] if ("select_cols" in kwargs.keys() and kwargs["select_cols"] is not None) else []

        # create grouping cols
        grouping_cols = []
        for c in select_cols:
            grouping_cols.append(c)
        grouping_cols.append(args[0])

    def group_by_key(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.group_by_key, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def arg_min(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.arg_min, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def arg_max(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.arg_max, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def aggregate(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.aggregate, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def filter(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.filter, self.requirements, *args, **kwargs))

    def exclude_filter(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.exclude_filter, self.requirements, *args, **kwargs))

    def any_col_with_cond_exists_filter(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.any_col_with_cond_exists_filter, self.requirements, *args, **kwargs))

    def any_col_with_cond_exists_exclude_filter(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.any_col_with_cond_exists_exclude_filter, self.requirements, *args, **kwargs))

    def all_col_with_cond_exists_filter(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.all_col_with_cond_exists_filter, self.requirements, *args, **kwargs))

    def all_col_with_cond_exists_exclude_filter(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.all_col_with_cond_exists_exclude_filter, self.requirements, *args, **kwargs))

    def transform(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.transform, self.requirements, *args, **kwargs))

    def transform_inline(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.transform_inline, self.requirements, *args, **kwargs))

    def transform_inline_log(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.transform_inline_log, self.requirements, *args, **kwargs))

    def transform_inline_log2(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.transform_inline_log2, self.requirements, *args, **kwargs))

    def transform_inline_log10(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.transform_inline_log10, self.requirements, *args, **kwargs))

    def transform_inline_log1p(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.transform_inline_log1p, self.requirements, *args, **kwargs))

    def transform_inline_log1p_base2(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.transform_inline_log1p_base2, self.requirements, *args, **kwargs))

    def transform_inline_log1p_base10(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.transform_inline_log1p_base10, self.requirements, *args, **kwargs))

    def rename(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.rename, self.requirements, *args, **kwargs))

    def get_header(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_header, self.requirements, *args, **kwargs))

    def get_data(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_data, self.requirements, *args, **kwargs))

    def get_header_map(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_header_map, self.requirements, *args, **kwargs))

    def num_rows(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.num_rows, self.requirements, *args, **kwargs))

    def num_cols(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.num_cols, self.requirements, *args, **kwargs))

    def get_size_in_bytes(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_size_in_bytes, self.requirements, *args, **kwargs))

    def size_in_bytes(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.size_in_bytes, self.requirements, *args, **kwargs))

    def size_in_mb(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.size_in_mb, self.requirements, *args, **kwargs))

    def size_in_gb(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.size_in_gb, self.requirements, *args, **kwargs))

    def get_header_fields(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_header_fields, self.requirements, *args, **kwargs))

    def get_columns(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_columns, self.requirements, *args, **kwargs))

    def get_column(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_column, self.requirements, *args, **kwargs))

    def columns(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.columns, self.requirements, *args, **kwargs))

    def get_column_index(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_column_index, self.requirements, *args, **kwargs))

    def export_to_maps(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.export_to_maps, self.requirements, *args, **kwargs))

    def to_maps(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.to_maps, self.requirements, *args, **kwargs))

    def to_int(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.to_int, self.requirements, *args, **kwargs))

    def to_numeric(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.to_numeric, self.requirements, *args, **kwargs))

    def add_seq_num(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.add_seq_num, self.requirements, *args, **kwargs))

    def show_transpose(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.show_transpose, self.requirements, *args, **kwargs))

    def show(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.show, self.requirements, *args, **kwargs))

    def show_sample(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.show_sample, self.requirements, *args, **kwargs))

    def col_as_array(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.col_as_array, self.requirements, *args, **kwargs))

    def col_as_float_array(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.col_as_float_array, self.requirements, *args, **kwargs))

    def col_as_int_array(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.col_as_int_array, self.requirements, *args, **kwargs))

    def col_as_array_uniq(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.col_as_array_uniq, self.requirements, *args, **kwargs))

    def col_as_array_uniq_non_empty(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.col_as_array_uniq_non_empty, self.requirements, *args, **kwargs))

    def cols_as_map(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.cols_as_map, self.requirements, *args, **kwargs))

    def sort(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.sort, self.requirements, *args, **kwargs))

    def reverse_sort(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.reverse_sort, self.requirements, *args, **kwargs))

    def numerical_sort(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.numerical_sort, self.requirements, *args, **kwargs))

    def reverse_numerical_sort(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.reverse_numerical_sort, self.requirements, *args, **kwargs))

    def reorder(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.reorder, self.requirements, *args, **kwargs))

    def reorder_reverse(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.reorder_reverse, self.requirements, *args, **kwargs))

    def reverse_reorder(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.reverse_reorder, self.requirements, *args, **kwargs))

    def noop(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.noop, self.requirements, *args, **kwargs))

    def to_df(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.to_df, self.requirements, *args, **kwargs))

    def to_simple_df(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.to_simple_df, self.requirements, *args, **kwargs))

    def export_to_df(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.export_to_df, self.requirements, *args, **kwargs))

    def to_json_records(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.to_json_records, self.requirements, *args, **kwargs))

    def to_csv(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.to_csv, self.requirements, *args, **kwargs))

    def url_encode_inline(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.url_encode_inline, self.requirements, *args, **kwargs))

    def url_decode_inline(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.url_decode_inline, self.requirements, *args, **kwargs))

    def url_decode_clean_inline(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.url_decode_clean_inline, self.requirements, *args, **kwargs))

    def url_encode(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.url_encode, self.requirements, *args, **kwargs))

    def url_decode(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.url_decode, self.requirements, *args, **kwargs))

    def resolve_url_encoded_cols(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.resolve_url_encoded_cols, self.requirements, *args, **kwargs))

    def resolve_url_encoded_list_cols(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.resolve_url_encoded_list_cols, self.requirements, *args, **kwargs))

    def resolve_all_url_encoded_cols(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.resolve_all_url_encoded_cols, self.requirements, *args, **kwargs))

    def union(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.union, self.requirements, *args, **kwargs))

    def difference(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.difference, self.requirements, *args, **kwargs))

    def add_const(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.add_const, self.requirements, *args, **kwargs))

    def add_const_if_missing(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.add_const_if_missing, self.requirements, *args, **kwargs))

    def add_empty_cols_if_missing(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.add_empty_cols_if_missing, self.requirements, *args, **kwargs))

    def add_row(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.add_row, self.requirements, *args, **kwargs))

    def add_map_as_row(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.add_map_as_row, self.requirements, *args, **kwargs))

    def assign_value(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.assign_value, self.requirements, *args, **kwargs))

    def concat_as_cols(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.concat_as_cols, self.requirements, *args, **kwargs))

    def add_col_prefix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.add_col_prefix, self.requirements, *args, **kwargs))

    def remove_suffix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.remove_suffix, self.requirements, *args, **kwargs))

    def add_prefix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.add_prefix, self.requirements, *args, **kwargs))

    def add_suffix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.add_suffix, self.requirements, *args, **kwargs))

    def rename_prefix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.rename_prefix, self.requirements, *args, **kwargs))

    def rename_suffix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.rename_suffix, self.requirements, *args, **kwargs))

    def remove_prefix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.remove_prefix, self.requirements, *args, **kwargs))

    def replace_prefix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.replace_prefix, self.requirements, *args, **kwargs))

    def replace_suffix(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.replace_suffix, self.requirements, *args, **kwargs))

    def sample(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.sample, self.requirements, *args, **kwargs))

    def sample_without_replacement(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.sample_without_replacement, self.requirements, *args, **kwargs))

    def sample_with_replacement(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.sample_with_replacement, self.requirements, *args, **kwargs))

    def sample_rows(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.sample_rows, self.requirements, *args, **kwargs))

    def sample_n(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.sample_n, self.requirements, *args, **kwargs))

    def sample_n_with_warn(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.sample_n_with_warn, self.requirements, *args, **kwargs))

    def sample_n_with_replacement(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.sample_n_with_replacement, self.requirements, *args, **kwargs))

    def sample_n_without_replacement(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.sample_n_without_replacement, self.requirements, *args, **kwargs))

    def sample_group_by_topk_if_reached_limit(self, *args, **kwargs):
        limit = args[0]
        grouping_cols = args[1]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_group_by_topk_if_reached_limit, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def warn_if_limit_reached(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.warn_if_limit_reached, self.requirements, *args, **kwargs))

    def cap_min_inline(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.cap_min_inline, self.requirements, *args, **kwargs))

    def cap_max_inline(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.cap_max_inline, self.requirements, *args, **kwargs))

    def cap_min(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.cap_min, self.requirements, *args, **kwargs))

    def cap_max(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.cap_max, self.requirements, *args, **kwargs))

    def copy(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.copy, self.requirements, *args, **kwargs))

    def sample_class(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.sample_class, self.requirements, *args, **kwargs))

    def sample_group_by_col_value(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_group_by_col_value, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def sample_group_by_max_uniq_values_exact(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_group_by_max_uniq_values_exact, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def sample_group_by_max_uniq_values_approx(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_group_by_max_uniq_values_approx, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def sample_group_by_max_uniq_values(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_group_by_max_uniq_values, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def sample_group_by_max_uniq_values_per_class(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_group_by_max_uniq_values_per_class, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def sample_group_by_key(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_group_by_key, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def sample_column_by_max_uniq_values(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_column_by_max_uniq_values, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def sample_class_by_min_class_count(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_class_by_min_class_count, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def sample_class_by_max_values(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_class_by_max_values, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def left_join(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.left_join, self.requirements, *args, **kwargs))

    def right_join(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.right_join, self.requirements, *args, **kwargs))

    def inner_join(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.inner_join, self.requirements, *args, **kwargs))

    def outer_join(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.outer_join, self.requirements, *args, **kwargs))

    def join(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.join, self.requirements, *args, **kwargs))

    def natural_join(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.natural_join, self.requirements, *args, **kwargs))

    def inner_map_join(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.inner_map_join, self.requirements, *args, **kwargs))

    def left_map_join(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.left_map_join, self.requirements, *args, **kwargs))

    def split_batches(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.split_batches, self.requirements, *args, **kwargs))

    def generate_key_hash(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.generate_key_hash, self.requirements, *args, **kwargs))

    def cumulative_sum(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.cumulative_sum, self.requirements, *args, **kwargs))

    def replicate_rows(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.replicate_rows, self.requirements, *args, **kwargs))

    def explode(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.explode, self.requirements, *args, **kwargs))

    def explode_json(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.explode_json, self.requirements, *args, **kwargs))

    def explode_json_v2(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.explode_json_v2, self.requirements, *args, **kwargs))

    def transpose(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.transpose, self.requirements, *args, **kwargs))

    def reverse_transpose(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.reverse_transpose, self.requirements, *args, **kwargs))

    def flatmap(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.flatmap, self.requirements, *args, **kwargs))

    def to_tuples(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.to_tuples, self.requirements, *args, **kwargs))

    def set_missing_values(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.set_missing_values, self.requirements, *args, **kwargs))

    def extend_class(self, *args, **kwargs):
        # find the corresponding hydra version of the class.
        hydra_class_ref = cluster_class_reflection.get_hydra_class(newclass, [sys.modules[__name__]])

        # check for None
        if (hydra_class_ref is None):
            raise Exception("HydraTSV: extend_class: not able to find hydra version for class: {}".format(newclass.__name__))

        # add the parameters
        hydra_class_obj = hydra_class_ref(self.header, self.data)
        hydra_class_obj.set_hydra_ctx(self.ctx)
        hydra_class_obj.set_hydra_operations(self.__copy_and_append_operations__(cluster_common_v2.ClusterExtendClassOperation(newclass, hydra_class_obj.requirements, *args, **kwargs)))

        # return
        return hydra_class_obj

    def extend_external_class(self, *args, **kwargs):
        raise Exception("Not implemented")

    def custom_func(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.custom_func, self.requirements, *args, **kwargs))

    def to_clipboard(self, *args, **kwargs):
        raise Exception("Not implemented")

    def filter_json_by_xpath(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.filter_json_by_xpath, self.requirements, *args, **kwargs))

    def get_col_index(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_col_index, self.requirements, *args, **kwargs))

    def get_hash(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.get_hash, self.requirements, *args, **kwargs))

    def is_empty(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.is_empty, self.requirements, *args, **kwargs))

    def has_empty_header(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.has_empty_header, self.requirements, *args, **kwargs))

    def write(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.write, self.requirements, *args, **kwargs))

    def show_custom_func(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.show_custom_func, self.requirements, *args, **kwargs))

    def show_group_count(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.show_group_count, self.requirements, *args, **kwargs))

    def show_select_func(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.show_select_func, self.requirements, *args, **kwargs))

    def show_transpose_custom_func(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.show_transpose_custom_func, self.requirements, *args, **kwargs))

    def print(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.print, self.requirements, *args, **kwargs))

    def print_stats(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.print_stats, self.requirements, *args, **kwargs))

    def warn(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.warn, self.requirements, *args, **kwargs))

    def warn_once(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.warn_once, self.requirements, *args, **kwargs))

    def get_max_size_cols_stats(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterSingletonOperation(TSV.get_max_size_cols_stats, self.requirements, *args, **kwargs))

    def sleep(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.sleep, self.requirements, *args, **kwargs))

    def split(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.split, self.requirements, *args, **kwargs))

    def split_str(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.split_str, self.requirements, *args, **kwargs))

    def sample_group_by_topk(self, *args, **kwargs):
        grouping_cols = args[0]
        op = cluster_common_v2.ClusterReduceOperation(grouping_cols, self.num_splits, TSV.sample_group_by_topk, self.requirements, *args, **kwargs)
        return self.__new_hydra_tsv__(op)

    def resolve_template_col(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.resolve_template_col, self.requirements, *args, **kwargs))

    def resolve_template_col_inline(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.resolve_template_col_inline, self.requirements, *args, **kwargs))

    def enable_info_mode(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.enable_info_mode, self.requirements, *args, **kwargs))

    def disable_info_mode(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.disable_info_mode, self.requirements, *args, **kwargs))

    def enable_debug_mode(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.enable_debug_mode, self.requirements, *args, **kwargs))

    def disable_debug_mode(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.disable_debug_mode, self.requirements, *args, **kwargs))

    def enable_trace_mode(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.enable_trace_mode, self.requirements, *args, **kwargs))

    def disable_trace_mode(self, *args, **kwargs):
        return self.__new_hydra_tsv__(cluster_common_v2.ClusterMapOperation(TSV.disable_trace_mode, self.requirements, *args, **kwargs))

class HydraHelper:
    def new_hydra_tsv(hydra_base, new_op):
        return HydraTSV(hydra_base.header, hydra_base.data, ctx = hydra_base.ctx, operations = hydra_base.__copy_and_append_operations__(new_op))

class HydraSparkJobShellExecutorTSV(HydraBaseTSV):
    def __init__(self, header, data):
        super().__init__(header, data)
        super().set_hydra_requirements([ClusterCapabilities.SPARK, ClusterCapabilities.SHELL])

    def execute(self, *args, **kwargs):
        return HydraHelper.new_hydra_tsv(self, cluster_common_v2.ClusterMapOperation(SparkJobShellExecutorTSV.execute, self.requirements, *args, **kwargs))

class HydraSplunkTSV(HydraBaseTSV):
    def __init__(self, header, data):
        super().__init__(header, data)
        super().set_hydra_requirements([ClusterCapabilities.SPLUNK])

    def get_events_par(self, *args, **kwargs):
        return HydraHelper.new_hydra_tsv(self, cluster_common_v2.ClusterMapOperation(SplunkTSV.get_events_par, self.requirements, *args, **kwargs))

    def get_events_parsed(self, *args, **kwargs):
        return HydraHelper.new_hydra_tsv(self, cluster_common_v2.ClusterMapOperation(SplunkTSV.get_events_parsed, self.requirements, *args, **kwargs))

