## API Documentation

This document is split into different sections grouping similar set of APIs together. Each API shows the most basic usage, and please refer to source for further details,
or the tutorial section where different apis are used together for different data analytics use cases.

### Get methods
    def get_data(self):
    def get_header(self):
    def get_header_fields(self):
    def get_header_map(self):
    def get_size_in_bytes(self):
    def num_cols(self):
    def num_rows(self):

### Static data transformations
    def add_col_prefix(self, cols, prefix):
    def add_const(self, col, value, inherit_message = ""):
    def add_const_if_missing(self, col, value, inherit_message = ""):
    def add_seq_num(self, new_col, inherit_message = ""):
    def assign_value(self, col, value, inherit_message = ""):
    def copy(self, col, newcol, inherit_message = ""):
    def replicate_rows(self, col, new_col, max_repl = 0):
    def set_missing_values(self, cols, default_val, inherit_message = ""):

### Arithmetic Operations
    def endswith(self, col, suffix, inherit_message = ""):
    def eq(self, col, value):
    def eq_float(self, col, value):
    def eq_int(self, col, value):
    def eq_str(self, col, value):
    def ge(self, col, value):
    def ge_str(self, col, value):
    def gt(self, col, value):
    def gt_str(self, col, value):
    def is_nonzero(self, col):
    def le(self, col, value):
    def le_str(self, col, value):
    def lt(self, col, value):
    def lt_str(self, col, value):
    def not_endswith(self, col, suffix, inherit_message = ""):
    def not_eq(self, col, value):
    def not_eq_str(self, col, value):
    def not_match(self, col, pattern, inherit_message = ""):
    def not_startswith(self, col, prefix, inherit_message = ""):
    def startswith(self, col, prefix, inherit_message = ""):
    def match(self, col, pattern, condition = True, inherit_message = ""):

### Filter and Transformtion
    def apply_precision(self, col, precision, inherit_message = ""):
    def cap_max(self, col, value, newcol):
    def cap_max_inline(self, col, value):
    def cap_min(self, col, value, newcol):
    def cap_min_inline(self, col, value):
    def exclude_filter(self, cols, func, inherit_message = ""):
    def filter(self, cols, func, include_cond = True, inherit_message = ""):
    def flatmap(self, col, func, new_col):
    def ratio(self, col1, col2, new_col, default = 0.0, precision = 6, inherit_message = ""):
    def ratio_const(self, col1, denominator, new_col, precision = 6, inherit_message = ""):
    def url_decode(self, col, newcol):
    def url_decode_inline(self, col_or_cols): 
    def url_encode(self, col, newcol):
    def url_encode_inline(self, col):
    def values_in(self, col, values, inherit_message = ""):
    def values_not_in(self, col, values, inherit_message = ""):
    def transform(self, cols, func, new_col_or_cols, use_array_notation = False, inherit_message = ""):
    def transform_inline(self, cols, func, inherit_message = ""):

### Sampling
    def sample(self, sampling_ratio, seed = 0):
    def sample_class(self, col, col_value, sampling_ratio, seed = 0):
    def sample_column_by_max_uniq_values(self, col, max_uniq_values, seed = 0, inherit_message = ""):
    def sample_group_by_col_value(self, grouping_cols, col, col_value, sampling_ratio, seed = 0, use_numeric = False):
    def sample_group_by_key(self, grouping_cols, sampling_ratio, seed = 0):
    def sample_group_by_max_uniq_values(self, grouping_cols, col, max_uniq_values, seed = 0):
    def sample_rows(self, n, seed = 0):


### Grouping and Aggregate
    def group_by_key(self, grouping_cols, agg_cols, agg_func, suffix = "", collapse = True, inherit_message = ""):
    def group_count(self, cols, new_col, collapse = True, inherit_message = ""):
    def explode(self, cols, exp_func, suffix = "", default_val = None, exclude_cols = True, inherit_message = ""):
    def explode_json(self, col, suffix = "", accepted_cols = None, excluded_cols = None, single_value_list_cols = None, transpose_col_groups = None,
    def aggregate(self, grouping_col_or_cols, agg_cols, agg_funcs, collapse = True, precision = 4, use_rolling = False, inherit_message = ""):
    def arg_max(self, grouping_cols, argcols, valcols, suffix = "arg_max", topk = 1, sep = "|", collapse = False):
    def arg_min(self, grouping_cols, argcols, valcols, suffix = "arg_min", topk = 1, sep = "|", collapse = False):
    def window_aggregate(self, win_col, agg_cols, agg_funcs, winsize, select_cols = None, sliding = False, collapse = True, suffix = "", precision = 2, inherit_message = ""):
    def distinct(self):
    def union(self, that_arr):
    def cumulative_sum(self, col, new_col, as_int = True):

### Join
    def join(self, that, lkeys, rkeys = None, join_type = "inner", lsuffix = "", rsuffix = "", default_val = "", def_val_map = None):
    def natural_join(self, that):
    def inner_join()
    def left_join()
    def right_join()

### Conversion to other data formats
    def to_json_records(self, new_col = "json"):
    def to_numeric(self, cols, precision = 4, inherit_message = ""):
    def to_string(self):
    def to_tuples(self, cols, inherit_message = ""):
    def to_csv(self, comma_replacement = ";"):
    def to_df()
    def export_to_df(self, n = -1):
    def cols_as_map(self, key_cols, value_cols):
    def col_as_array(self, col):
    def col_as_array_uniq(self, col):
    def col_as_float_array(self, col):
    def col_as_int_array(self, col):
    def export_to_maps(self):

### Column add, delete, rename, prefix and suffix
    def drop(self, col_or_cols, inherit_message = ""):
    def drop_if_exists(self, col_or_cols, inherit_message = ""):
    def concat_as_cols(self, that):
    def rename(self, col, new_col):
    def rename_suffix(self, old_suffix, new_suffix):
    def rename_prefix(self, old_suffix, new_suffix):
    def remove_suffix(self, old_suffix, new_suffix):
    def rename_prefix(self, old_suffix, new_suffix):

### Sort and Reorder 
    def reorder(self, cols, inherit_message = ""):
    def reorder_reverse(self, cols, inherit_message = ""):
    def sort(self, cols, reverse = False, reorder = False, all_numeric = None):
    def reverse_sort(self, cols, reverse = False, reorder = False, all_numeric = None):

### Select Columns
    def select(self, col_or_cols, inherit_message = ""):

### Select Rows Slice
    def skip(self, count):
    def last(self, count):
    def take(self, count):

### Transpose from row to column format
    def transpose(self, num_rows = 1):
    def reverse_transpose(self, grouping_cols, transpose_key, transpose_cols, default_val = ""):

### Extending to other derived classes
    def extend_class(self, newclass):

### Get basic summary and stats
    def noop(self, funcstr):
    def print(self, msg):
    def print_stats(self, msg):
    def show(self, n = 100, max_col_width = 40, label = None):
    def show_transpose(self, n = 100, max_col_width = 40, label = None):
    def validate(self):

