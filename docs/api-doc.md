## API Documentation

This document is split into different sections grouping similar set of APIs together. Each API shows the most basic usage, and please refer to source for further details,
or the tutorial section where different apis are used together for different data analytics use cases.

### Introduction
The APIs are divided into three groups:
    1. Data Ingestion: This is provided through static methods available in the *tsv_data_analytics.tsv* package. Eg. tsv.read()
    1. Data Transformation and Analytics: These are availabe in the *tsv_data_analytics.tsv.TSV* class.
    1. Visualization, Statistics, Machine Learning and Advanced Functionalities: These are part of extensions package, either provided in this repository as
       tsv_data_analytics_ext or any 3rd party package.

Note: Only the tsv_data_analytics.tsv package should be used. The other packages under tsv_data_analytics are not meant to be called directly, and are subject to change without notice.

#### 1. Data Ingestion
Following APIs are provided to get data loaded as TSV and also debug different methods.

#### Read / Write from Local FileSystem, S3 or Web
    def tsv.read
    def tsv.write
    def tsv.merge

#### Helper Methods to Check for File Existence
    def tsv.exists

#### Logging and Debugging
    def tsv.debug
    def tsv.info
    def tsv.error
    def tsv.warn
    def tsv.enable_debug_mode
    def tsv.disable_debug_mode
    def tsv.set_report_progress_perc
    def tsv.set_report_progress_min_thresh

#### 2. Data Transformation and Analytics
These APIs are part of TSV class. Once the data is loaded as TSV, all these methods can be used.

#### Basic Summary
    def num_cols
    def num_rows
    def get_header_fields
    def get_size_in_bytes

### Pretty Print
    def show
    def show_transpose

#### Static Data Transformations
    def add_seq_num
    def assign_value
    def copy
    def replicate_rows
    def set_missing_values

#### Arithmetic Comparison 
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

#### String Comparison
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

#### Basic Filter and Transformation
    def filter
    def exclude_filter
    def values_in
    def values_not_in
    def transform
    def transform_inline

#### Advanced Filter and Transformation
    def cap_max
    def cap_max_inline
    def cap_min
    def cap_min_inline
    def apply_precision
    def explode
    def flatmap
    def ratio
    def ratio_const

#### URL Encoding and Decoding
    def url_decode
    def url_decode_inline 
    def url_encode
    def url_encode_inline

#### Sampling Rows
    def sample
    def sample_rows
    def sample_with_replacement
    def sample_without_replacement

#### Sampling Groups
    def sample_class
    def sample_group_by_col_value
    def sample_group_by_key
    def sample_group_by_max_uniq_values
    def sample_group_by_max_uniq_values_per_class

#### Simple Grouping and Aggregation
    def aggregate
    def distinct
    def group_count
    def cumulative_sum

#### Advanced Grouping and Aggregation
    def arg_max
    def arg_min
    def group_by_key
    def window_aggregate

#### Generic JSON Parsing
    def explode_json

#### Join
    def join
    def inner_join()
    def left_join()
    def right_join()
    def natural_join

#### Column Add, Delete, Rename
    def drop
    def drop_cols
    def drop_if_exists
    def concat_as_cols
    def rename

#### Add or Rename Column Prefix and Suffixes
    def rename_suffix
    def rename_prefix
    def remove_suffix
    def rename_prefix
    def add_prefix

#### Sorting
    def sort
    def reverse_sort

#### Reorder Columns
    def reorder
    def reverse_reorder

#### Select Columns
    def select

#### Select Rows Slice
    def skip
    def skip_rows
    def last
    def take

#### Transpose from Row to Column Format
    def transpose
    def reverse_transpose

#### Extending to Other Derived Classes
    def extend_class

#### Basic Getters 
    def get_data
    def get_header
    def get_header_map
    def to_string
    def validate

#### Conversion to Other Data Formats
    def to_json_records
    def to_numeric
    def to_tuples
    def to_csv
    def to_df
    def export_to_df
    def export_to_maps

#### Getting Column Values as Arrays
    def cols_as_map
    def col_as_array
    def col_as_array_uniq
    def col_as_float_array
    def col_as_int_array

#### Merging Multiple TSVs
    def union

#### Appending Rows and Columns
    def add_row
    def add_map_as_row
    def add_const
    def add_const_if_missing

### Print Messages and Stats 
    def print
    def print_stats

### Debugging
    def noop

### 3. Visualization, Statistics and Machine Learning
Any functionality that needs extensive 3rd party libraries like matplotlib, seaborn or scikit as provided as extension packages. Not all extension packages might be 
relevant for all users, and please refer to the documentation section of individual packages for further details. Here are some basic explained for visualization.

#### Line Chart
    def linechart

#### Scatter Plot
    def scatterplot

#### Histogram
    def histogram

#### Density
    def density

#### Bar Chart
    def barchart

#### Boxplot 
    def boxplot 

#### Correlation Heatmap
    def corr_heatmap 

#### Pair Plot
    def pairplot

