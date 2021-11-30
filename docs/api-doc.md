## Introduction
This library is designed to make simple and robust APIs to do complex processing easily. This document describes basic functionality, and provides simple usage of the APis. The APIs are divided into three groups:

1. **Data Ingestion**: This is provided through static methods available in the **tsv** package.

2. **Data Transformation**: These are availabe in the **tsv.TSV** class.

3. **Visualization and Advanced Functionalities**: These are part of different extension packages.

## Working with Data Having 100s of Columns
Existing libraries for data analysis were designed atleast 5-10 years back when there were usually 10-20 columns in a dataset. Modern datasets have columns in the 100s or even 1000s. This become
more evident when datasets are joined for enrichement, and new features are created adding more and more columns. 

This library provides simple APIs to work with such wide column datasets. Methods like select(), drop(), sort() take regular expression for getting the list of columns.

For example, selecting all columns that start with prefix _petal_ from the iris dataset:
```
>>> xtsv.select("petal.*") 
```

Both single or set of columns and/or regular expressions can be specified wherever applicable.
Another example to mix and match column names and regular expressions:
```
>>> xtsv.select(["petal_length", "sepal.*"])
>>> xtsv.sort("petal.*")
```

All advanced api like **aggregate()** follows simple naming convention to create new names with appropriate prefix or suffixes to help with selecting them in groups.
```
>>> xtsv.aggregate("class", ["petal_length"], [min]).show()
class          	petal_length:min
Iris-setosa    	           1.000
Iris-versicolor	           3.000
Iris-virginica 	           4.500 
```

# API Documentation 

## Data Ingestion

#### Read and Write from Local FileSystem, S3 or Web
    def read(paths)
    def write(tsv_obj, path)
    def merge(tsv_objs)
    def exists(path)

#### Enable / Disable Debug Information 
    def enable_debug_mode
    def disable_debug_mode
    def set_report_progress_perc
    def set_report_progress_min_thresh

## Data Transformation and Analytics
These APIs are part of TSV class. Once the data is loaded as TSV, all these methods can be used.

#### Basic Summary
    def num_cols
    def num_rows
    def get_columns 
    def get_size_in_bytes

#### Pretty Print
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
    def ge_int
    def gt_int
    def le_int
    def lt_int
    def eq_float
    def ge_float
    def gt_float
    def le_float
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

## Visualization, Statistics and Machine Learning
Any functionality that needs extensive 3rd party libraries like matplotlib, seaborn or scikit as provided as extension packages. Not all extension packages might be 
relevant for all users, and please refer to the documentation section of individual packages for further details. Here are some basic explained for visualization.

#### Basic Plots 
    def linechart
    def scatterplot
    def histogram
    def density
    def barchart
    def boxplot 

#### Advanced Plots
    def corr_heatmap 
    def pairplot

