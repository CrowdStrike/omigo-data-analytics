"""TSV Class"""
import re
import math
import pandas as pd
import random
import json
from omigo_core import utils, tsvutils, udfs, dataframe 
import sys
import time
import numpy as np
from io import StringIO

class TSV:
    """This is the main data processing class to apply different filter and transformation functions
    on tsv data and get the results. The design is more aligned with functional programming where
    each step generates a new copy of the data"""

    header = None
    data = None

    # constructor
    def __init__(self, *args, **kwargs):
        # initialize header and data
        self.header = header
        self.data = data

    def to_string(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).to_string(*args, **kwargs)

    def get_content_as_string(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).get_content_as_string(*args, **kwargs)

    def validate(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).validate(*args, **kwargs)

    def has_col(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).has_col(*args, **kwargs)

    def select(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).select(*args, **kwargs)

    def values_not_in(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).values_not_in(*args, **kwargs)

    def values_in(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).values_in(*args, **kwargs)

    def not_match(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).not_match(*args, **kwargs)

    def not_regex_match(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).not_regex_match(*args, **kwargs)

    def match(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).match(*args, **kwargs)

    def regex_match(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).regex_match(*args, **kwargs)

    def not_eq(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).not_eq(*args, **kwargs)

    def eq(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).eq(*args, **kwargs)

    def eq_int(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).eq_int(*args, **kwargs)

    def eq_float(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).eq_float(*args, **kwargs)

    def eq_str(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).eq_str(*args, **kwargs)

    def not_eq_int(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).not_eq_int(*args, **kwargs)

    def not_eq_float(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).not_eq_float(*args, **kwargs)

    def not_eq_str(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).not_eq_str(*args, **kwargs)

    def is_nonzero(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).is_nonzero(*args, **kwargs)

    def is_nonzero_int(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).is_nonzero_int(*args, **kwargs)

    def is_nonzero_float(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).is_nonzero_float(*args, **kwargs)

    def lt_str(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).lt_str(*args, **kwargs)

    def le_str(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).le_str(*args, **kwargs)

    def gt_str(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).gt_str(*args, **kwargs)

    def ge_str(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).ge_str(*args, **kwargs)

    def gt(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).gt(*args, **kwargs)

    def gt_int(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).gt_int(*args, **kwargs)

    def gt_float(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).gt_float(*args, **kwargs)

    def ge(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).ge(*args, **kwargs)

    def ge_int(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).ge_int(*args, **kwargs)

    def ge_float(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).ge_float(*args, **kwargs)

    def lt(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).lt(*args, **kwargs)

    def lt_int(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).lt_int(*args, **kwargs)

    def lt_float(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).lt_float(*args, **kwargs)

    def le(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).le(*args, **kwargs)

    def le_int(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).le_int(*args, **kwargs)

    def le_float(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).le_float(*args, **kwargs)

    def startswith(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).startswith(*args, **kwargs)

    def not_startswith(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).not_startswith(*args, **kwargs)

    def endswith(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).endswith(*args, **kwargs)

    def not_endswith(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).not_endswith(*args, **kwargs)

    def is_empty_str(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).is_empty_str(*args, **kwargs)

    def is_nonempty_str(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).is_nonempty_str(*args, **kwargs)

    def replace_str_inline(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).replace_str_inline(*args, **kwargs)

    def group_count(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).group_count(*args, **kwargs)

    def ratio(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).ratio(*args, **kwargs)

    def ratio_const(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).ratio_const(*args, **kwargs)

    def apply_precision(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).apply_precision(*args, **kwargs)

    def skip(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).skip(*args, **kwargs)

    def skip_rows(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).skip_rows(*args, **kwargs)

    def last(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).last(*args, **kwargs)

    def take(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).take(*args, **kwargs)

    def distinct(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).distinct(*args, **kwargs)

    def distinct_cols(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).distinct_cols(*args, **kwargs)
       
    def drop(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).drop(*args, **kwargs)

    def drop_cols(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).drop_cols(*args, **kwargs)

    def drop_cols_with_prefix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).drop_cols_with_prefix(*args, **kwargs)

    def drop_cols_with_suffix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).drop_cols_with_suffix(*args, **kwargs)

    def drop_if_exists(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).drop_if_exists(*args, **kwargs)

    def drop_cols_if_exists(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).drop_cols_if_exists(*args, **kwargs)

    def drop_empty_cols(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).drop_empty_cols(*args, **kwargs)

    def drop_empty_rows(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).drop_empty_rows(*args, **kwargs)
 
    def window_aggregate(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).window_aggregate(*args, **kwargs)

    def group_by_key(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).group_by_key(*args, **kwargs)

    def arg_min(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).arg_min(*args, **kwargs)

    def arg_max(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).arg_max(*args, **kwargs)

    def aggregate(self, *args, **kwargs):
        return dataframe.aggregate(*args, **kwargs)

    def filter(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).filter(*args, **kwargs)

    def exclude_filter(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).exclude_filter(*args, **kwargs)

    def any_col_with_cond_exists_filter(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).any_col_with_cond_exists_filter(*args, **kwargs)

    def any_col_with_cond_exists_exclude_filter(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).any_col_with_cond_exists_exclude_filter(*args, **kwargs)

    def all_col_with_cond_exists_filter(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).all_col_with_cond_exists_filter(*args, **kwargs)

    def all_col_with_cond_exists_exclude_filter(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).all_col_with_cond_exists_exclude_filter(*args, **kwargs)

    def transform(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).transform(*args, **kwargs)

    def transform_inline(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).transform_inline(*args, **kwargs)

    def transform_inline_log(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).transform_inline_log(*args, **kwargs)

    def transform_inline_log2(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).transform_inline_log2(*args, **kwargs)

    def transform_inline_log10(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).transform_inline_log10(*args, **kwargs)

    def transform_inline_log1p(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).transform_inline_log1p(*args, **kwargs)

    def transform_inline_log1p_base2(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).transform_inline_log1p_base2(*args, **kwargs)
        
    def transform_inline_log1p_base10(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).transform_inline_log1p_base10(*args, **kwargs)
        
    def rename(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).rename(*args, **kwargs)

    def get_header(self, *args, **kwargs):
        return self.header

    def get_data(self, *args, **kwargs):
        return self.data

    def get_header_map(self, *args, **kwargs):
        raise Exception("get_header_map: not supported")

    def num_rows(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).num_rows(*args, **kwargs)

    def num_cols(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).num_cols(*args, **kwargs)

    def get_size_in_bytes(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).get_size_in_bytes(*args, **kwargs)

    def size_in_bytes(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).size_in_bytes(*args, **kwargs)

    def size_in_mb(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).size_in_mb(*args, **kwargs)

    def size_in_gb(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).size_in_gb(*args, **kwargs)

    def get_header_fields(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).get_header_fields(*args, **kwargs)

    def get_columns(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).get_columns(*args, **kwargs)

    def get_column(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).get_column(*args, **kwargs)

    def columns(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).columns(*args, **kwargs)

    def get_column_index(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).columns(*args, **kwargs)

    def export_to_maps(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).export_to_maps(*args, **kwargs)

    def to_maps(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).to_maps(*args, **kwargs)

    def to_int(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).to_int(*args, **kwargs)

    def to_numeric(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).to_numeric(*args, **kwargs)

    def add_seq_num(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).add_seq_num(*args, **kwargs)

    def show_transpose(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).show_transpose(*args, **kwargs)

    def show(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).show(*args, **kwargs)

    def show_sample(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).show_sample(*args, **kwargs)

    def col_as_array(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).col_as_array(*args, **kwargs)

    def col_as_float_array(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).col_as_float_array(*args, **kwargs)

    def col_as_int_array(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).col_as_int_array(*args, **kwargs)

    def col_as_array_uniq(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).col_as_array_uniq(*args, **kwargs)

    def col_as_array_uniq_non_empty(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).col_as_array_uniq_non_empty(*args, **kwargs)

    def cols_as_map(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).cols_as_map(*args, **kwargs)

    def sort(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sort(*args, **kwargs)

    def reverse_sort(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).reverse_sort(*args, **kwargs)

    def numerical_sort(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).numerical_sort(*args, **kwargs)

    def reverse_numerical_sort(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).reverse_numerical_sort(*args, **kwargs)

    def reorder(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).reorder(*args, **kwargs)

    def reorder_reverse(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).reorder_reverse(*args, **kwargs)

    def reverse_reorder(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).reverse_reorder(*args, **kwargs)

    def noop(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).noop(*args, **kwargs)

    def to_df(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).to_df(*args, **kwargs)

    def to_simple_df(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).to_simple_df(*args, **kwargs)

    def export_to_df(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).export_to_df(*args, **kwargs)

    def to_json_records(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).to_json_records(*args, **kwargs)

    def to_csv(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).to_csv(*args, **kwargs)

    def url_encode_inline(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).url_encode_inline(*args, **kwargs)

    def url_decode_inline(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).url_decode_inline(*args, **kwargs)

    def url_decode_clean_inline(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).url_decode_clean_inline(*args, **kwargs)

    def url_encode(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).url_encode(*args, **kwargs)

    def url_decode(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).url_decode(*args, **kwargs)

    def resolve_url_encoded_cols(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).resolve_url_encoded_cols(*args, **kwargs)

    def resolve_url_encoded_list_cols(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).resolve_url_encoded_list_cols(*args, **kwargs)

    def resolve_all_url_encoded_cols(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).resolve_all_url_encoded_cols(*args, **kwargs)

    def union(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).union(*args, **kwargs)

    def difference(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).difference(*args, **kwargs)

    def add_const(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).add_const(*args, **kwargs)

    def add_const_if_missing(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).add_const_if_missing(*args, **kwargs)

    def add_empty_cols_if_missing(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).add_empty_cols_if_missing(*args, **kwargs)

    def add_row(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).add_row(*args, **kwargs)

    def add_map_as_row(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).add_map_as_row(*args, **kwargs)

    def assign_value(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).assign_value(*args, **kwargs)

    def concat_as_cols(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).concat_as_cols(*args, **kwargs)

    def add_col_prefix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).add_col_prefix(*args, **kwargs)

    def remove_suffix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).remove_suffix(*args, **kwargs)

    def add_prefix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).add_prefix(*args, **kwargs)

    def add_suffix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).add_suffix(*args, **kwargs)

    def rename_prefix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).rename_prefix(*args, **kwargs)

    def rename_suffix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).rename_suffix(*args, **kwargs)

    def remove_prefix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).remove_prefix(*args, **kwargs)

    def replace_prefix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).replace_prefix(*args, **kwargs)

    def replace_suffix(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).replace_suffix(*args, **kwargs)

    def sample(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample(*args, **kwargs)

    def sample_without_replacement(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_without_replacement(*args, **kwargs)

    def sample_with_replacement(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_with_replacement(*args, **kwargs)

    def sample_rows(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_rows(*args, **kwargs)

    def sample_n(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_n(*args, **kwargs)

    def sample_n_with_warn(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_n_with_warn(*args, **kwargs)

    def sample_n_with_replacement(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_n_with_replacement(*args, **kwargs)
 
    def sample_n_without_replacement(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_n_without_replacement(*args, **kwargs)
 
    def sample_group_by_topk_if_reached_limit(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_group_by_topk_if_reached_limit(*args, **kwargs)

    def warn_if_limit_reached(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).warn_if_limit_reached(*args, **kwargs)

    def cap_min_inline(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).cap_min_inline(*args, **kwargs)

    def cap_max_inline(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).cap_max_inline(*args, **kwargs)

    def cap_min(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).cap_min(*args, **kwargs)

    def cap_max(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).cap_max(*args, **kwargs)

    def copy(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).copy(*args, **kwargs)

    def sample_class(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_class(*args, **kwargs)

    def sample_group_by_col_value(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_group_by_col_value(*args, **kwargs)

    def sample_group_by_max_uniq_values_exact(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_group_by_max_uniq_values_exact(*args, **kwargs)

    def sample_group_by_max_uniq_values_approx(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_group_by_max_uniq_values_approx(*args, **kwargs)

    def sample_group_by_max_uniq_values(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_group_by_max_uniq_values(*args, **kwargs)

    def sample_group_by_max_uniq_values_per_class(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_group_by_max_uniq_values_per_class(*args, **kwargs)

    def sample_group_by_key(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_group_by_key(*args, **kwargs)

    def sample_column_by_max_uniq_values(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_column_by_max_uniq_values(*args, **kwargs)

    def sample_class_by_min_class_count(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_class_by_min_class_count(*args, **kwargs)

    def sample_class_by_max_values(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_class_by_max_values(*args, **kwargs)

    def left_join(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).left_join(*args, **kwargs)

    def right_join(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).right_join(*args, **kwargs)

    def inner_join(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).inner_join(*args, **kwargs)

    def outer_join(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).outer_join(*args, **kwargs)

    def join(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).join(*args, **kwargs)

    def natural_join(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).natural_join(*args, **kwargs)

    def inner_map_join(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).inner_map_join(*args, **kwargs)

    def left_map_join(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).left_map_join(*args, **kwargs)

    def split_batches(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).split_batches(*args, **kwargs)

    def generate_key_hash(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).generate_key_hash(*args, **kwargs)

    def cumulative_sum(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).cumulative_sum(*args, **kwargs)

    def replicate_rows(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).replicate_rows(*args, **kwargs)

    def explode(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).explode(*args, **kwargs)

    def explode_json(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).explode_json(*args, **kwargs)

    def explode_json_v2(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).explode_json_v2(*args, **kwargs)

    def transpose(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).transpose(*args, **kwargs)

    def reverse_transpose(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).reverse_transpose(*args, **kwargs)

    def flatmap(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).flatmap(*args, **kwargs)

    def to_tuples(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).to_tuples(*args, **kwargs)

    def set_missing_values(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).set_missing_values(*args, **kwargs)

    def extend_class(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).extend_class(*args, **kwargs)

    def extend_external_class(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).extend_external_class(*args, **kwargs)

    def custom_func(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).custom_func(*args, **kwargs)

    def to_clipboard(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).to_clipboard(*args, **kwargs)

    def filter_json_by_xpath(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).filter_json_by_xpath(*args, **kwargs)

    def get_col_index(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).get_col_index(*args, **kwargs)

    def get_hash(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).get_hash(*args, **kwargs)

    def is_empty(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).is_empty(*args, **kwargs)

    def has_empty_header(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).has_empty_header(*args, **kwargs)

    def write(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).write(*args, **kwargs)

    def show_custom_func(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).show_custom_func(*args, **kwargs)

    def show_group_count(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).show_group_count(*args, **kwargs)

    def show_select_func(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).show_select_func(*args, **kwargs)

    def show_transpose_custom_func(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).show_transpose_custom_func(*args, **kwargs)

    def print(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).print(*args, **kwargs)

    def print_stats(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).print_stats(*args, **kwargs)

    def warn(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).warn(*args, **kwargs)

    def warn_once(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).warn_once(*args, **kwargs)

    def get_max_size_cols_stats(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).get_max_size_cols_stats(*args, **kwargs)
                
    def sleep(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sleep(*args, **kwargs)

    def split(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).split(*args, **kwargs)
 
    def split_str(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).split_str(*args, **kwargs)

    def sample_group_by_topk(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).sample_group_by_topk(*args, **kwargs)

    def resolve_template_col(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).resolve_template_col(*args, **kwargs)

    def resolve_template_col_inline(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).resolve_template_col_inline(*args, **kwargs)

    def enable_info_mode(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).enable_info_mode(*args, **kwargs)

    def disable_info_mode(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).disable_info_mode(*args, **kwargs)

    def enable_debug_mode(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).enable_debug_mode(*args, **kwargs)

    def disable_debug_mode(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).disable_debug_mode(*args, **kwargs)

    def enable_trace_mode(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).enable_trace_mode(*args, **kwargs)
        
    def disable_trace_mode(self, *args, **kwargs):
        return dataframe.from_tsv(self.header, self.data).disable_trace_mode(*args, **kwargs)
        
def get_version(*args, **kwargs):
    return dataframe.get_version(*args, **kwargs)

def get_func_name(*args, **kwargs):
    return dataframe.get_func_name(*args, **kwargs)

def read(*args, **kwargs):
    return dataframe.read(*args, **kwargs)

def write(*args, **kwargs):
    return dataframe.write(*args, **kwargs)

def merge(*args, **kwargs):
    return dataframe.merge(*args, **kwargs)

def merge_union(*args, **kwargs):
    return dataframe.merge_union(*args, **kwargs)

def merge_intersect(*args, **kwargs):
    return dataframe.merge_intersect(*args, **kwargs)

def exists(*args, **kwargs):
    return dataframe.exists(*args, **kwargs)

def from_df(*args, **kwargs):
    return dataframe.from_df(*args, **kwargs)

def from_maps(*args, **kwargs):
    return dataframe.from_maps(*args, **kwargs)

def enable_debug_mode(*args, **kwargs):
    return dataframe.enable_debug_mode(*args, **kwargs)

def disable_debug_mode(*args, **kwargs):
    return dataframe.disable_debug_mode(*args, **kwargs)

def set_report_progress_perc(*args, **kwargs):
    return dataframe.set_report_progress_perc(*args, **kwargs)

def set_report_progress_min_thresh(*args, **kwargs):
    return dataframe.set_report_progress_min_thresh(*args, **kwargs)

def newWithCols(*args, **kwargs):
    return dataframe.newWithCols(*args, **kwargs)

def new_with_cols(*args, **kwargs):
    return dataframe.new_with_cols(*args, **kwargs)

def create_empty(*args, **kwargs):
    return dataframe.create_empty(*args, **kwargs)

