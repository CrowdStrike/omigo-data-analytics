## API Documentation

This document is split into different sections grouping similar set of APIs together. Each API shows the most basic usage, and please refer to source for further details,
or the tutorial section where different apis are used together for different data analytics use cases.

### Basic Getters 
    get_data
    get_header
    get_header_fields
    get_header_map
    get_size_in_bytes
    num_cols
    num_rows

### Static Data Transformations
    add_col_prefix
    add_const
    add_const_if_missing
    add_seq_num
    assign_value
    copy
    replicate_rows
    set_missing_values

### Arithmetic Operations
    eq_int
    eq_float
    ge_int
    ge_float
    gt_int
    gt_float
    le_int
    le_float
    lt_int
    lt_float
    is_nonzero_int
    is_nonzero_float

### String Comparison
    eq_str
    ge_str
    gt_str
    le_str
    lt_str
    startswith
    endswith
    match
    not_eq_str
    not_startswith
    not_endswith
    not_match

### Basic Filter and Transformtion
    filter
    exclude_filter
    values_in
    values_not_in
    transform
    transform_inline

### Advanced Filter and Transformation
    cap_max
    cap_max_inline
    cap_min
    cap_min_inline
    apply_precision
    flatmap
    ratio
    ratio_const

# URL Encoding and Decoding
    url_decode
    url_decode_inline 
    url_encode
    url_encode_inline

### Sampling Rows
    sample
    sample_rows
    sample_with_replacement
    sample_without_replacement

### Sampling Groups
    sample_class
    sample_group_by_col_value
    sample_group_by_key
    sample_group_by_max_uniq_values
    sample_group_by_max_uniq_values_per_class

### Simple Grouping and Aggregation
    group_by_key
    aggregate
    distinct
    group_count
    cumulative_sum

### Advanced Grouping and Aggregation
    arg_max
    arg_min
    explode
    window_aggregate

### Generic JSON Parsing
    explode_json(self, col, suffix = "", accepted_cols = None, excluded_cols = None, single_value_list_cols = None, transpose_col_groups = None,

### Join
    join
    inner_join()
    left_join()
    right_join()
    natural_join

### Column add, delete, rename, prefix and suffix
    drop
    drop_if_exists
    concat_as_cols
    rename
    rename_suffix
    rename_prefix
    remove_suffix
    rename_prefix

### Sort
    sort
    reverse_sort

### Reorder Columns
    reorder
    reverse_reorder

### Select Columns
    select

### Select Rows Slice
    skip
    last
    take

### Transpose from row to column format
    transpose
    reverse_transpose

### Extending to other derived classes
    extend_class

### Get basic summary and stats
    noop
    print
    print_stats
    show
    show_transpose
    validate

### Conversion to other data formats
    to_json_records
    to_numeric
    to_string
    to_tuples
    to_csv
    to_df()
    export_to_df
    cols_as_map
    col_as_array
    col_as_array_uniq
    col_as_float_array
    col_as_int_array
    export_to_maps

### Merging Multiple
    union
