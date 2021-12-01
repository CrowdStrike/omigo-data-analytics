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

### Read and Write from Local FileSystem, S3 or Web
   - def **read**(paths): Reads the data present in the list of file paths.
    
   - def **write**(tsv_obj, path): Writes the tsv object to the specified path.
    
   - def **merge**(tsv_objs): Merges the list of tsv objects into one.
    
   - def **exists**(path): Check whether the specified path exists or not. Useful for implementing fast forwarding.

Supported File Formats: tsv, simple csv and gzip/zip compressed versions.

#### Examples
```
>>> xtsv = read("data/iris.tsv")
>>> xtsv = read("s3://bucket-name/path/iris.tsv")
>>> xtsv = read(["data/file1.tsv.gz", "data/file2.tsv.gz"]
>>> xtsv = read("https://github.com/CrowdStrike/tsv-data-analytics/raw/main/data/iris.tsv")
>>> write(xtsv, "data/output_file.tsv.gz")
>>> ytsv = merge([xtsv1, xtsv2])
>>> exists("data/iris.tsv")
>>> exists("s3://bucket-name/path/iris.tsv")
```

## Data Transformation and Analytics
These APIs are part of TSV class. Once the data is loaded as TSV, all these methods can be used. The optional arguments are shown in _italics_.

### Basic Summary
   - **num_cols**(): Returns the number of columns in the tsv object.
   - **num_rows**(): Returns the number of rows in the tsv object.
   - **columns**(): Returns the list of columns in the tsv object.
   - **size_in_bytes**(): Returns the size of the tsv object in bytes.

### Pretty Print
   - **show**(_n_, _max_col_width_, _title_): Pretty prints the first 'n' rows, each column restricted to max_col_width, and title which is displayed at the top.
   - **show_transpose**(_n_, _title_): Transposes the tsv object and does pretty print.

#### Examples
```
>>> xtsv.show(3)
>>>
sepal_length	sepal_width	petal_length	petal_width	class      
5.1         	        3.5	         1.4	        0.2	Iris-setosa
4.9         	        3.0	         1.4	        0.2	Iris-setosa
4.7         	        3.2	         1.3	        0.2	Iris-setosa
```

```
>>> xtsv.show_transpose(3)
>>>
col_name    	row:1      	row:2      	row:3      
sepal_length	5.1        	4.9        	4.7        
sepal_width 	3.5        	3.0        	3.2        
petal_length	1.4        	1.4        	1.3        
petal_width 	0.2        	0.2        	0.2        
class       	Iris-setosa	Iris-setosa	Iris-setosa
```

### Arithmetic Comparison 
   - **eq_int**(col, value): Returns all rows where the int value of _col_ is equal to _value_.
   - **ge_int**(col, value): Returns all rows where the int value of _col_ is less than or equal to _value_.
   - **gt_int**(col, value): Returns all rows where the int value of _col_ is greater than _value_.
   - **le_int**(col, value): Returns all rows where the int value of _col_ is less than or equal to _value_.
   - **lt_int**(col, value): Returns all rows where the int value of _col_ is less than _value_.
   - **eq_float**(col, value): Returns all rows where the float value of _col_ is equal to _value_.
   - **ge_float**(col, value): Returns all rows where the float value of _col_ is less than or equal to _value_.
   - **gt_float**(col, value): Returns all rows where the float value of _col_ is greater than _value_.
   - **le_float**(col, value): Returns all rows where the float value of _col_ is less than or equal to _value_.
   - **lt_float**(col, value): Returns all rows where the float value of _col_ is less than _value_.
   - **is_nonzero**(col): Returns all rows where the float value of _col_ is not zero.

#### Examples
```
>>> xtsv.gt_float("petal_length", 1.4)
>>> xtsv.eq_float("sepal_length", 5.1)
```

### String Comparison
   - **eq_str**(col, value): Returns rows where string value of _col_ is equal to _value_. 
   - **ge_str**(col, value): Returns rows where string value of _col_ is greater than or equal to _value_.
   - **gt_str**(col, value): Returns rows where string value of _col_ is greater than _value_.
   - **le_str(**col, value): Returns rows where string value of _col_ is less than or equal to _value_.
   - **lt_str**(col, value): Returns rows where string value of _col_ is less than _value_.
   - **startswith**(col, value): Returns rows where string value of _col_ starts with _value_.
   - **endswith**(col, value): Returns rows where string value of _col_ ends with  _value_.
   - **match**(col, value): Returns rows where string value of _col_ matches the regular expression in _value_.
   - **not_eq_str**(col, value): Returns rows where string value of _col_ not equal to  _value_.
   - **not_startswith**(col, value): Returns rows where string value of _col_ does not start with  _value_.
   - **not_endswith**(col, value): Returns rows where string value of _col_ does not end with _value_.
   - **not_match**(col, value): Returns rows where string value of _col_ does not match regular expression in _value_.
   - **not_regex_match**(col, value): Returns rows where string value of _col_ does not match regular expression in _value_.

#### Examples
```
>>> xtsv.eq_str("class", "Iris-setosa")
>>> xtsv.match("class", ".*setosa")
```
 
### Basic Filter and Transformation
   - **filter**(cols, lambda_func):
   - **values_in**(col, vals): TBD
   - **transform**(cols, lambda_func, output_col_names):  TBD
   - **transform_inline**(cols, func): TBD 
   - **values_not_in**(col, vals): This is negation of values_in() api.
   - **exclude_filter**(cols, func): This is negation of filter() api.

### Advanced Filter and Transformation
   - cap_max
   - cap_max_inline
   - cap_min
   - cap_min_inline
   - apply_precision
   - explode
   - flatmap
   - ratio
   - ratio_const

### URL Encoding and Decoding
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
    def aggregate
    def distinct
    def group_count
    def cumulative_sum

### Advanced Grouping and Aggregation
    def arg_max
    def arg_min
    def group_by_key
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

### Add or Rename Column Prefix and Suffixes
    def rename_suffix
    def rename_prefix
    def remove_suffix
    def rename_prefix
    def add_prefix

### Sorting
    def sort
    def reverse_sort

### Reorder Columns
    def reorder
    def reverse_reorder

### Select Columns
    def select

### Select Rows Slice
    def skip
    def skip_rows
    def last
    def take

### Transpose from Row to Column Format
    def transpose
    def reverse_transpose

### Extending to Other Derived Classes
    def extend_class

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

### Static Data Transformations
    def add_seq_num
    def assign_value
    def copy
    def replicate_rows
    def set_missing_values

### Enable / Disable Debug Information 
    def enable_debug_mode
    def disable_debug_mode
    def set_report_progress_perc
    def set_report_progress_min_thresh

## Visualization, Statistics and Machine Learning
Any functionality that needs extensive 3rd party libraries like matplotlib, seaborn or scikit as provided as extension packages. Not all extension packages might be 
relevant for all users, and please refer to the documentation section of individual packages for further details. Here are some basic explained for visualization.

### Basic Plots 
    def linechart
    def scatterplot
    def histogram
    def density
    def barchart
    def boxplot 

### Advanced Plots
    def corr_heatmap 
    def pairplot

