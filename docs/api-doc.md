## API Documentation

This document is split into different sections grouping similar set of APIs together. Each API shows the most basic usage, and please refer to source for further details,
or the tutorial section where different apis are used together for different data analytics use cases.

### Basic Getters 
    def get_data
    def get_header
    def get_header_fields
    def get_header_map
    def get_size_in_bytes
    def num_cols
    def num_rows
    def to_string

### Static Data Transformations
    def add_col_prefix
    def add_seq_num
    def assign_value
    def copy
    def replicate_rows
    def set_missing_values

### Arithmetic Operations
    def eq_int
    def eq_float
    def ge_int
    def ge_float
    def gt_int
    def gt_float
    def le_int
    def le_float
    def lt_int
    def lt_float
    def is_nonzero_int
    def is_nonzero_float

### String Comparison
    def eq_str
    def ge_str
    def gt_str
    def le_str
    def lt_str
    def startswith
    def endswith
    def match
    def regex_match
    def not_eq_str
    def not_startswith
    def not_endswith
    def not_match
    def not_regex_match

### Basic Filter and Transformation
    def filter
    def exclude_filter
    def values_in
    def values_not_in
    def transform
    def transform_inline

### Advanced Filter and Transformation
    def cap_max
    def cap_max_inline
    def cap_min
    def cap_min_inline
    def apply_precision
    def explode
    def flatmap
    def ratio
    def ratio_const

# URL Encoding and Decoding
    def url_decode
    def url_decode_inline 
    def url_encode
    def url_encode_inline

### Sampling Rows
    def sample
    def sample_rows
    def sample_with_replacement
    def sample_without_replacement

### Sampling Groups
    def sample_class
    def sample_group_by_col_value
    def sample_group_by_key
    def sample_group_by_max_uniq_values
    def sample_group_by_max_uniq_values_per_class

### Simple Grouping and Aggregation
    def group_by_key
    def aggregate
    def distinct
    def group_count
    def cumulative_sum

### Advanced Grouping and Aggregation
    def arg_max
    def arg_min
    def window_aggregate

### Generic JSON Parsing
    def explode_json

### Join
    def join
    def inner_join()
    def left_join()
    def right_join()
    def natural_join

### Column Add, Delete, Rename
    def drop
    def drop_cols
    def drop_if_exists
    def concat_as_cols
    def rename

### Renaming Column Prefix and Suffix
    def rename_suffix
    def rename_prefix
    def remove_suffix
    def rename_prefix

### Sort
    def sort
    def reverse_sort

### Reorder Columns
    def reorder
    def reverse_reorder

### Select Columns
    def select

### Select Rows Slice
    def skip
    def last
    def take

### Transpose from Row to Column Format
    def transpose
    def reverse_transpose

### Extending to Other Derived Classes
    def extend_class

### Basic Summary and Stats
    def noop
    def print
    def print_stats
    def show
    def show_transpose
    def validate

### Conversion to Other Data Formats
    def to_json_records
    def to_numeric
    def to_tuples
    def to_csv
    def to_df
    def export_to_df
    def export_to_maps

### Getting Column Values as Arrays
    def cols_as_map
    def col_as_array
    def col_as_array_uniq
    def col_as_float_array
    def col_as_int_array

### Merging Multiple TSVs
    def union

### Appending Rows and Columns
    def add_row
    def add_map_as_row
    def add_const
    def add_const_if_missing
