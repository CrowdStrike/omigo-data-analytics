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
>>> x.select("petal.*") 
```

Both single or set of columns and/or regular expressions can be specified wherever applicable.
Another example to mix and match column names and regular expressions:
```
>>> x.select(["petal_length", "sepal.*"])
>>> x.sort("petal.*")
```

All advanced api like **aggregate()** follows simple naming convention to create new names with appropriate prefix or suffixes to help with selecting them in groups.
```
>>> x.aggregate("class", ["petal_length"], [min]).show()
class          	petal_length:min
Iris-setosa    	           1.000
Iris-versicolor	           3.000
Iris-virginica 	           4.500 
```

# API Documentation 

## A. Data Ingestion

### Read and Write from Local FileSystem, S3 or Web
   - **tsv.read**(paths): Reads the data present in the list of file paths.
   - **tsv.write**(tsv_obj, path): Writes the tsv object to the specified path.
   - **tsv.merge**(tsv_objs): Merges the list of tsv objects into one.
   - **tsv.exists**(path): Check whether the specified path exists or not. Useful for implementing fast forwarding.

Supported File Formats: tsv, simple csv and gzip/zip compressed versions.

#### Examples
```
>>> x = tsv.read("data/iris.tsv")
>>> x = tsv.read("s3://bucket-name/path/iris.tsv")
>>> x = tsv.read(["data/file1.tsv.gz", "data/file2.tsv.gz"]
>>> x = tsv.read("https://github.com/CrowdStrike/tsv-data-analytics/raw/main/data/iris.tsv")
>>> tsv.write(x, "data/output_file.tsv.gz")
>>> ytsv = tsv.merge([x1, x2])
>>> flag = tsv.exists("data/iris.tsv")
>>> flag = tsv.exists("s3://bucket-name/path/iris.tsv")
```

## B. Data Transformation and Analysis
These APIs are part of TSV class. Once the data is loaded as TSV, all these methods can be used. 

**Note**: The optional arguments are shown in _italics_.

### Basic Summary
   - **num_cols**(): Returns the number of columns in the tsv object.
   - **num_rows**(): Returns the number of rows in the tsv object.
   - **columns**(): Returns the list of columns in the tsv object.
   - **size_in_bytes**(): Returns the size of the tsv object in bytes.

### Pretty Print
   - **show**(_n_, _max_col_width_, _title_): Pretty prints the first 'n' rows, each upto max_col_width wide and title which is displayed at the top.
   - **show_transpose**(_n_, _title_): Transposes the tsv object and does pretty print.

#### Examples
```
>>> x.show(3)
>>>
sepal_length	sepal_width	petal_length	petal_width	class      
5.1         	        3.5	         1.4	        0.2	Iris-setosa
4.9         	        3.0	         1.4	        0.2	Iris-setosa
4.7         	        3.2	         1.3	        0.2	Iris-setosa
```

```
>>> x.show_transpose(3)
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
>>> x.gt_float("petal_length", 1.4)
>>> x.eq_float("sepal_length", 5.1)
```

### String Comparison
   - **eq_str**(col, value): Returns rows where string value of _col_ is equal to _value_. 
   - **ge_str**(col, value): Returns rows where string value of _col_ is greater than or equal to _value_.
   - **gt_str**(col, value): Returns rows where string value of _col_ is greater than _value_.
   - **le_str**(col, value): Returns rows where string value of _col_ is less than or equal to _value_.
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
>>> x.eq_str("class", "Iris-setosa")
>>> x.match("class", ".*setosa")
```
 
### Basic Filter and Transformation
   - **values_in**(col, _values_): Returns rows where the value of the _col_ is one of the provided _values_.
   - **filter**(cols, lambda_func): Returns rows that satisfy applying lambda function on the values of the given columns.
   - **transform**(cols, lambda_func, output_cols): Applies lambda function to the given _cols_. The lambda function can return single or multiple values. The _output_cols_ should match the list of values in the output.
   - **transform_inline**(cols, func): This applies the lambda function on each of the given _cols_ and returns new values under the same column names. 
   - **values_not_in**(col, list_of_values): This is negation of _values_in()_ api.
   - **exclude_filter**(cols, lambda_func): This is negation of _filter()_ api.
   - **ratio**(col1, col2, new_col, _default = 0_): Returns the ratio of _col1_ / _col2_ as _new_col_. If denominator is 0, then uses _default_ as the default value.

#### Examples
**values_in()** to take rows with specific values for column _class_.
```
>>> x.values_in("class", ["Iris-setosa", "Iris-versicolor"])
```

**filter()** with single and multiple input columns.
```
>>> x.filter(["sepal_length"], lambda x: float(x) != 0)
>>> x.filter(["petal_length", "petal_width"], lambda x, y: float(x) > 1.4 and float(y) > 2.0)
```

**transform()** with multiple input columns.
```
>>> x.transform(["petal_length", "petal_width"], lambda x,y: float(x) * float(y), "petal_length_and_width")

sepal_length	sepal_width	petal_length	petal_width	class      	petal_length_and_width
5.1         	        3.5	      1.4000	     0.2000	Iris-setosa	                0.2800
4.9         	        3.0	      1.4000	     0.2000	Iris-setosa	                0.2800
4.7         	        3.2	      1.3000	     0.2000	Iris-setosa	                0.2600
```

**transform()** with multiple output columns.
```
>>> x.transform("petal_length", lambda x: (float(x)*2, float(x)*3), ["petal_length_2x", "petal_length_3x"]).show(3)

sepal_length	sepal_width	petal_length	petal_width	class      	petal_length_2x	petal_length_3x
5.1         	        3.5	      1.4000	     0.2000	Iris-setosa	         2.8000	         4.2000
4.9         	        3.0	      1.4000	     0.2000	Iris-setosa	         2.8000	         4.2000
4.7         	        3.2	      1.3000	     0.2000	Iris-setosa	         2.6000	         3.9000
```

### Advanced Filter and Transformation
   - **explode**
   - **flatmap**
   - **to_tuples**

### URL Encoding and Decoding
   - **url_encode**
   - **url_decode**
   - **url_encode_inline**
   - **url_decode_inline**

### Sampling Rows
   - **sample**
   - **sample_rows**
   - **sample_with_replacement**
   - **sample_without_replacement**

### Sampling Groups
   - **sample_class**
   - **sample_group_by_col_value**
   - **sample_group_by_key**
   - **sample_group_by_max_uniq_values**
   - **sample_group_by_max_uniq_values_per_class**

### Simple Grouping and Aggregation
   - **aggregate**
   - **distinct**

### Advanced Grouping and Aggregation
   - **arg_max**
   - **arg_min**
   - **group_by_key**
   - **window_aggregate**

### Generic JSON Parsing
   - **explode_json**

### Join
   - **inner_join**
   - **left_join**
   - **right_join**

### Column Add, Delete, Rename
   - **drop**
   - **drop_if_exists**
   - **concat_as_cols**
   - **rename**

### Add or Rename Column Prefix and Suffixes
   - **rename_suffix**
   - **rename_prefix**
   - **remove_suffix**
   - **rename_prefix**
   - **add_prefix**

### Sorting
   - **sort**
   - **reverse_sort**

### Reorder Columns
   - **reorder**

### Select Columns
   - **select**

### Select Rows Slice
   - **skip**
   - **last**
   - **take**

### Transpose from Row to Column Format
   - **transpose**

### Extending to Other Derived Classes
   - **extend_class**

### Conversion to Other Data Formats
   - **to_json_records**
   - **to_csv**
   - **to_df**
   - **export_to_df**
   - **export_to_maps**

### Getting Column Values as Arrays
   - **cols_as_map**
   - **col_as_array**
   - **col_as_array_uniq**
   - **col_as_float_array**
   - **col_as_int_array**

### Merging Multiple TSVs
   - **union**

### Appending Rows and Columns
   - **add_row**
   - **add_map_as_row**
   - **add_const**
   - **add_const_if_missing**

### Static Data Transformations
   - **add_seq_num**
   - **assign_value**
   - **copy**
   - **replicate_rows**
   - **set_missing_values**

## C. Visualization, Statistics and Machine Learning
Any functionality that needs extensive 3rd party libraries like matplotlib, seaborn or scikit as provided as extension packages. Not all extension packages might be 
relevant for all users, and please refer to the documentation section of individual packages for further details. Here are some basic explained for visualization.

### Basic Plots 
   - **linechart**
   - **scatterplot**
   - **histogram**
   - **density**
   - **barchart**
   - **boxplot**

### Advanced Plots
   - **corr_heatmap** 
   - **pairplot**

### D. Enable / Disable Debug Messages 
   - **tsv.enable_debug_mode**
   - **tsv.disable_debug_mode**
   - **tsv.set_report_progress_perc**
   - **tsv.set_report_progress_min_thresh**

