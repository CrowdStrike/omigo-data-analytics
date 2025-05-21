"""TSV Class"""
from omigo_core import utils

# Deprecated
class TSV:
    """This is the main data processing class to apply different filter and transformation functions
    on tsv data and get the results. The design is more aligned with functional programming where
    each step generates a new copy of the data"""

    header = None
    data = None

    # constructor
    def __init__(self, header, data):
        self.header = header
        self.data = data
        self.header_fields = header.split("\t")
        utils.warn_once("Deprecated")

    def to_string(self, *args, **kwargs):
        return self

    def get_content_as_string(self, *args, **kwargs):
        return self

    def validate(self, *args, **kwargs):
        return self

    def has_col(self, *args, **kwargs):
        return self

    def select(self, *args, **kwargs):
        return self

    def values_not_in(self, *args, **kwargs):
        return self

    def values_in(self, *args, **kwargs):
        return self

    def not_match(self, *args, **kwargs):
        return self

    def not_regex_match(self, *args, **kwargs):
        return self

    def match(self, *args, **kwargs):
        return self

    def regex_match(self, *args, **kwargs):
        return self

    def not_eq(self, *args, **kwargs):
        return self

    def eq(self, *args, **kwargs):
        return self

    def eq_int(self, *args, **kwargs):
        return self

    def eq_float(self, *args, **kwargs):
        return self

    def eq_str(self, *args, **kwargs):
        return self

    def not_eq_int(self, *args, **kwargs):
        return self

    def not_eq_float(self, *args, **kwargs):
        return self

    def not_eq_str(self, *args, **kwargs):
        return self

    def is_nonzero(self, *args, **kwargs):
        return self

    def is_nonzero_int(self, *args, **kwargs):
        return self

    def is_nonzero_float(self, *args, **kwargs):
        return self

    def lt_str(self, *args, **kwargs):
        return self

    def le_str(self, *args, **kwargs):
        return self

    def gt_str(self, *args, **kwargs):
        return self

    def ge_str(self, *args, **kwargs):
        return self

    def gt(self, *args, **kwargs):
        return self

    def gt_int(self, *args, **kwargs):
        return self

    def gt_float(self, *args, **kwargs):
        return self

    def ge(self, *args, **kwargs):
        return self

    def ge_int(self, *args, **kwargs):
        return self

    def ge_float(self, *args, **kwargs):
        return self

    def lt(self, *args, **kwargs):
        return self

    def lt_int(self, *args, **kwargs):
        return self

    def lt_float(self, *args, **kwargs):
        return self

    def le(self, *args, **kwargs):
        return self

    def le_int(self, *args, **kwargs):
        return self

    def le_float(self, *args, **kwargs):
        return self

    def startswith(self, *args, **kwargs):
        return self

    def not_startswith(self, *args, **kwargs):
        return self

    def endswith(self, *args, **kwargs):
        return self

    def not_endswith(self, *args, **kwargs):
        return self

    def is_empty_str(self, *args, **kwargs):
        return self

    def is_nonempty_str(self, *args, **kwargs):
        return self

    def replace_str_inline(self, *args, **kwargs):
        return self

    def group_count(self, *args, **kwargs):
        return self

    def ratio(self, *args, **kwargs):
        return self

    def ratio_const(self, *args, **kwargs):
        return self

    def apply_precision(self, *args, **kwargs):
        return self

    def skip(self, *args, **kwargs):
        return self

    def skip_rows(self, *args, **kwargs):
        return self

    def last(self, *args, **kwargs):
        return self

    def take(self, *args, **kwargs):
        return self

    def distinct(self, *args, **kwargs):
        return self

    def distinct_cols(self, *args, **kwargs):
        return self

    def drop(self, *args, **kwargs):
        return self

    def drop_cols(self, *args, **kwargs):
        return self

    def drop_cols_with_prefix(self, *args, **kwargs):
        return self

    def drop_cols_with_suffix(self, *args, **kwargs):
        return self

    def drop_if_exists(self, *args, **kwargs):
        return self

    def drop_cols_if_exists(self, *args, **kwargs):
        return self

    def drop_empty_cols(self, *args, **kwargs):
        return self

    def drop_empty_rows(self, *args, **kwargs):
        return self

    def window_aggregate(self, *args, **kwargs):
        return self

    def group_by_key(self, *args, **kwargs):
        return self

    def arg_min(self, *args, **kwargs):
        return self

    def arg_max(self, *args, **kwargs):
        return self

    def aggregate(self, *args, **kwargs):
        return self 

    def filter(self, *args, **kwargs):
        return self

    def exclude_filter(self, *args, **kwargs):
        return self

    def any_col_with_cond_exists_filter(self, *args, **kwargs):
        return self

    def any_col_with_cond_exists_exclude_filter(self, *args, **kwargs):
        return self

    def all_col_with_cond_exists_filter(self, *args, **kwargs):
        return self

    def all_col_with_cond_exists_exclude_filter(self, *args, **kwargs):
        return self

    def transform(self, *args, **kwargs):
        return self

    def transform_inline(self, *args, **kwargs):
        return self

    def transform_inline_log(self, *args, **kwargs):
        return self

    def transform_inline_log2(self, *args, **kwargs):
        return self

    def transform_inline_log10(self, *args, **kwargs):
        return self

    def transform_inline_log1p(self, *args, **kwargs):
        return self

    def transform_inline_log1p_base2(self, *args, **kwargs):
        return self

    def transform_inline_log1p_base10(self, *args, **kwargs):
        return self

    def rename(self, *args, **kwargs):
        return self

    def get_header(self, *args, **kwargs):
        return self.header

    def get_data(self, *args, **kwargs):
        return self.data

    def num_rows(self, *args, **kwargs):
        return self

    def num_cols(self, *args, **kwargs):
        return self

    def get_size_in_bytes(self, *args, **kwargs):
        return self

    def size_in_bytes(self, *args, **kwargs):
        return self

    def size_in_mb(self, *args, **kwargs):
        return self

    def size_in_gb(self, *args, **kwargs):
        return self

    def get_header_fields(self, *args, **kwargs):
        return self.header_fields

    def get_columns(self, *args, **kwargs):
        return self.header_fields

    def get_column(self, *args, **kwargs):
        return self

    def columns(self, *args, **kwargs):
        return self

    def get_column_index(self, *args, **kwargs):
        return self

    def export_to_maps(self, *args, **kwargs):
        return self

    def to_maps(self, *args, **kwargs):
        return self

    def to_int(self, *args, **kwargs):
        return self

    def to_numeric(self, *args, **kwargs):
        return self

    def add_seq_num(self, *args, **kwargs):
        return self

    def show_transpose(self, *args, **kwargs):
        return self

    def show(self, *args, **kwargs):
        return self

    def show_sample(self, *args, **kwargs):
        return self

    def col_as_array(self, *args, **kwargs):
        return self

    def col_as_float_array(self, *args, **kwargs):
        return self

    def col_as_int_array(self, *args, **kwargs):
        return self

    def col_as_array_uniq(self, *args, **kwargs):
        return self

    def col_as_array_uniq_non_empty(self, *args, **kwargs):
        return self

    def cols_as_map(self, *args, **kwargs):
        return self

    def sort(self, *args, **kwargs):
        return self

    def reverse_sort(self, *args, **kwargs):
        return self

    def numerical_sort(self, *args, **kwargs):
        return self

    def reverse_numerical_sort(self, *args, **kwargs):
        return self

    def reorder(self, *args, **kwargs):
        return self

    def reorder_reverse(self, *args, **kwargs):
        return self

    def reverse_reorder(self, *args, **kwargs):
        return self

    def noop(self, *args, **kwargs):
        return self

    def to_df(self, *args, **kwargs):
        return self

    def to_simple_df(self, *args, **kwargs):
        return self

    def export_to_df(self, *args, **kwargs):
        return self

    def to_json_records(self, *args, **kwargs):
        return self

    def to_csv(self, *args, **kwargs):
        return self

    def url_encode_inline(self, *args, **kwargs):
        return self

    def url_decode_inline(self, *args, **kwargs):
        return self

    def url_encode(self, *args, **kwargs):
        return self

    def url_decode(self, *args, **kwargs):
        return self

    def resolve_url_encoded_cols(self, *args, **kwargs):
        return self

    def resolve_url_encoded_list_cols(self, *args, **kwargs):
        return self

    def resolve_all_url_encoded_cols(self, *args, **kwargs):
        return self

    def union(self, *args, **kwargs):
        return self

    def difference(self, *args, **kwargs):
        return self

    def add_const(self, *args, **kwargs):
        return self

    def add_const_if_missing(self, *args, **kwargs):
        return self

    def add_empty_cols_if_missing(self, *args, **kwargs):
        return self

    def add_row(self, *args, **kwargs):
        return self

    def add_map_as_row(self, *args, **kwargs):
        return self

    def assign_value(self, *args, **kwargs):
        return self

    def concat_as_cols(self, *args, **kwargs):
        return self

    def add_col_prefix(self, *args, **kwargs):
        return self

    def remove_suffix(self, *args, **kwargs):
        return self

    def add_prefix(self, *args, **kwargs):
        return self

    def add_suffix(self, *args, **kwargs):
        return self

    def rename_prefix(self, *args, **kwargs):
        return self

    def rename_suffix(self, *args, **kwargs):
        return self

    def remove_prefix(self, *args, **kwargs):
        return self

    def replace_prefix(self, *args, **kwargs):
        return self

    def replace_suffix(self, *args, **kwargs):
        return self

    def sample(self, *args, **kwargs):
        return self

    def sample_without_replacement(self, *args, **kwargs):
        return self

    def sample_with_replacement(self, *args, **kwargs):
        return self

    def sample_rows(self, *args, **kwargs):
        return self

    def sample_n(self, *args, **kwargs):
        return self

    def sample_n_with_warn(self, *args, **kwargs):
        return self

    def sample_n_with_replacement(self, *args, **kwargs):
        return self

    def sample_n_without_replacement(self, *args, **kwargs):
        return self

    def sample_group_by_topk_if_reached_limit(self, *args, **kwargs):
        return self

    def warn_if_limit_reached(self, *args, **kwargs):
        return self

    def cap_min_inline(self, *args, **kwargs):
        return self

    def cap_max_inline(self, *args, **kwargs):
        return self

    def cap_min(self, *args, **kwargs):
        return self

    def cap_max(self, *args, **kwargs):
        return self

    def copy(self, *args, **kwargs):
        return self

    def sample_class(self, *args, **kwargs):
        return self

    def sample_group_by_col_value(self, *args, **kwargs):
        return self

    def sample_group_by_max_uniq_values_exact(self, *args, **kwargs):
        return self

    def sample_group_by_max_uniq_values_approx(self, *args, **kwargs):
        return self

    def sample_group_by_max_uniq_values(self, *args, **kwargs):
        return self

    def sample_group_by_max_uniq_values_per_class(self, *args, **kwargs):
        return self

    def sample_group_by_key(self, *args, **kwargs):
        return self

    def sample_column_by_max_uniq_values(self, *args, **kwargs):
        return self

    def sample_class_by_min_class_count(self, *args, **kwargs):
        return self

    def sample_class_by_max_values(self, *args, **kwargs):
        return self

    def left_join(self, *args, **kwargs):
        return self

    def right_join(self, *args, **kwargs):
        return self

    def inner_join(self, *args, **kwargs):
        return self

    def outer_join(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def natural_join(self, *args, **kwargs):
        return self

    def inner_map_join(self, *args, **kwargs):
        return self

    def left_map_join(self, *args, **kwargs):
        return self

    def split_batches(self, *args, **kwargs):
        return self

    def generate_key_hash(self, *args, **kwargs):
        return self

    def cumulative_sum(self, *args, **kwargs):
        return self

    def replicate_rows(self, *args, **kwargs):
        return self

    def explode(self, *args, **kwargs):
        return self

    def explode_json(self, *args, **kwargs):
        return self

    def explode_json_v2(self, *args, **kwargs):
        return self

    def transpose(self, *args, **kwargs):
        return self

    def reverse_transpose(self, *args, **kwargs):
        return self

    def flatmap(self, *args, **kwargs):
        return self

    def to_tuples(self, *args, **kwargs):
        return self

    def set_missing_values(self, *args, **kwargs):
        return self

    def extend_class(self, *args, **kwargs):
        return self

    def extend_external_class(self, *args, **kwargs):
        return self

    def custom_func(self, *args, **kwargs):
        return self

    def to_clipboard(self, *args, **kwargs):
        return self

    def filter_json_by_xpath(self, *args, **kwargs):
        return self

    def get_col_index(self, *args, **kwargs):
        return self

    def get_hash(self, *args, **kwargs):
        return self

    def is_empty(self, *args, **kwargs):
        return self

    def has_empty_header(self, *args, **kwargs):
        return self

    def write(self, *args, **kwargs):
        return self

    def show_custom_func(self, *args, **kwargs):
        return self

    def show_group_count(self, *args, **kwargs):
        return self

    def show_select_func(self, *args, **kwargs):
        return self

    def show_transpose_custom_func(self, *args, **kwargs):
        return self

    def print(self, *args, **kwargs):
        return self

    def print_stats(self, *args, **kwargs):
        return self

    def warn(self, *args, **kwargs):
        return self

    def warn_once(self, *args, **kwargs):
        return self

    def get_max_size_cols_stats(self, *args, **kwargs):
        return self

    def sleep(self, *args, **kwargs):
        return self

    def split(self, *args, **kwargs):
        return self

    def split_str(self, *args, **kwargs):
        return self

    def sample_group_by_topk(self, *args, **kwargs):
        return self

    def resolve_template_col(self, *args, **kwargs):
        return self

    def resolve_template_col_inline(self, *args, **kwargs):
        return self
