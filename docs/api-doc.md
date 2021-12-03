# Overview 
This library is designed to do complex data processing easily. This document describes basic functionality, and provides simple usage of the APIs. More detailed options are documented in the source code.

The APIs are divided into three groups:

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
>>> x.drop("petal.*")
```

All advanced apis like **aggregate()** follows simple naming convention to create new names with appropriate prefix or suffixes to help with selecting them in groups.
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
   - **tsv.read**(paths): Reads the data present in the list of file paths and urls.
   - **tsv.write**(tsv_obj, path): Writes the tsv object to the specified path.
   - **tsv.merge**(tsv_objs): Merges the list of tsv objects into one.
   - **tsv.exists**(path): Check whether the specified path exists or not. Useful for implementing fast forwarding.
   - **tsv.from_df**(df): Converts a pandas dataframe to TSV.
   - **tsv.from_maps**(mps): Converts the given array of maps to TSV

Supported File Formats: tsv, simple csv and gzip/zip compressed versions.

*Examples*
```
>>> x = tsv.read("data/iris.tsv")
>>> x = tsv.read("s3://bucket-name/path/iris.tsv")
>>> x = tsv.read(["data/file1.tsv.gz", "data/file2.tsv.gz"]
>>> x = tsv.read("https://github.com/CrowdStrike/tsv-data-analytics/raw/main/data/iris.tsv")
```
Saving TSV object to file
```
>>> tsv.write(x, "data/output_file.tsv.gz")
```
Merging mutliple TSV objects
```
>>> ytsv = tsv.merge([x1, x2])
```
Checking for existence of file path
```
>>> flag = tsv.exists("data/iris.tsv")
>>> flag = tsv.exists("s3://bucket-name/path/iris.tsv")
```
Converting pandas dataframe to TSV
```
>>> import pandas as pd
>>> df = pandas.read_csv("data/iris.csv")
>>> x = tsv.from_df(df)
>>> x.show(3)

sepal_length	sepal_width	petal_length	petal_width	class      
5.1         	        3.5	         1.4	        0.2	Iris-setosa
4.9         	        3.0	         1.4	        0.2	Iris-setosa
4.7         	        3.2	         1.3	        0.2	Iris-setosa
```

## B. Data Transformation and Analysis
These APIs are part of TSV class. Once the data is loaded as TSV, all these methods can be used. 

**Note**: The optional arguments are shown in _italics_.

### 1. Basic Summary
   - **num_cols**(): Returns the number of columns in the tsv object.
   - **num_rows**(): Returns the number of rows in the tsv object.
   - **columns**(): Returns the list of columns in the tsv object.
   - **size_in_bytes**(): Returns the size of the tsv object in bytes.

### 2. Pretty Print
   - **show**(_n_, _max_col_width_, _title_): Pretty prints the first 'n' rows, each upto max_col_width wide and title which is displayed at the top.
   - **show_transpose**(_n_, _title_): Transposes the tsv object and does pretty print.

*Examples*
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

### 3. Select Columns
   - **select**(cols): Selects the given columns which can be a single column, pattern or an array of columns.

*Examples*
```
>>> x.select(["class", "petal.*"]).show(3)

class      	petal_length	petal_width
Iris-setosa	         1.4	        0.2
Iris-setosa	         1.4	        0.2
Iris-setosa	         1.3	        0.2
```

### 4. Select Rows Slice
   - **skip**(n): Skips the first _n_ rows.
   - **last**(n): Takes the last _n_ rows.
   - **take**(n): Takes the first _n_ rows.

*Examples*
```
>>> x.take(2).show()

sepal_length	sepal_width	petal_length	petal_width	class      
5.1         	        3.5	         1.4	        0.2	Iris-setosa
4.9         	        3.0	         1.4	        0.2	Iris-setosa
```

### 5. Arithmetic Comparison 
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

*Examples*
```
>>> x.gt_float("petal_length", 1.4)
>>> x.eq_float("sepal_length", 5.1)
```

### 6. String Comparison
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

*Examples*
```
>>> x.eq_str("class", "Iris-setosa")
>>> x.match("class", ".*setosa")
```
 
### 7. Basic Filtering and Transformation
   - **values_in**(col, _values_): Returns rows where the value of the _col_ is one of the provided _values_.
   - **filter**(cols, lambda_func): Returns rows that satisfy lambda function on the values of the given columns.
   - **transform**(cols, lambda_func, output_cols): Applies lambda function to the given _cols_. The lambda function can return single or multiple values. The _output_cols_ should match the list of values in the output.
   - **transform_inline**(cols, func): This applies the lambda function on each of the given _cols_ and returns new values under the same column names. 
   - **values_not_in**(col, values): This is negation of _values_in()_ api.
   - **exclude_filter**(cols, lambda_func): This is negation of _filter()_ api.
   - **ratio**(col1, col2, new_col, _default = 0_): Returns the ratio of _col1_ / _col2_ as _new_col_. If denominator is 0, then returns _default_ value.

*Examples*
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
>>> x.transform(["petal_length", "petal_width"], lambda x,y: float(x) * float(y), "petal_length_and_width").show(3)

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

### 8. Advanced Filtering and Transformation
   - **explode**(cols, lambda_func, prefix): This method creates a map of the _cols_ in each row, and passes to the given _lambda_func_. The output of the _lambda_func_
should be an array of maps where each output map will get added as new row with the key-values as col values.
As the name suggests, this api can explode the number of rows and columns, and should be used carefully.

*Examples*
```
>>> def my_explode_func(mp):
>>>     mps = []
>>>     mps.append({"dim_name": "petal_length", "dim_value": str(mp["petal_length"])})
>>>     mps.append({"dim_name": "sepal_length", "dim_value": str(mp["sepal_length"])})
>>>     return mps
>>>
>>> x.explode(["petal_length", "sepal_length"], my_explode_func, "len_col").show(3)

sepal_width	petal_width	class      	len_col:dim_name	len_col:dim_value
3.5        	        0.2	Iris-setosa	petal_length    	              1.4
3.5        	        0.2	Iris-setosa	sepal_length    	              5.1
3.0        	        0.2	Iris-setosa	petal_length    	              1.4

```

### 9. URL Encoding and Decoding
   - **url_encode**(col, new_col): URL encode the values in _col_ and create a new column with name _new_col_
   - **url_decode**(col, new_col): URL decode the values in _col_ and create a new column with name _new_col_

### 10. Sampling Rows
   - **sample**(sampling_ratio, _seed_): Randomly samples _perc_ rows from the data using the given seed. 
   - **sample_n**(n, _seed_): Randomly samples _n_ rows from the data using the given seed.

*Examples*
```
>>> x.sample_n(3).show()

sepal_length	sepal_width	petal_length	petal_width	class          
5.1         	        2.5	         3.0	        1.1	Iris-versicolor
7.3         	        2.9	         6.3	        1.8	Iris-virginica 
5.4         	        3.7	         1.5	        0.2	Iris-setosa 
```

### 11. Sampling Groups
   - **sample_class**(col, col_value, sampling_ratio, _seed_): This api randomly samples _sampling_ratio_ rows for column _col_ but only for row that have value as _col_value_.
Useful for doing downsampling of specific class only.
   - **sample_group_by_col_value**(grouping_cols, col, col_value, sampling_ratio, _seed_): This api groups data using the _grouping_cols_, and then does sampling of
given column and its value within that group. Useful for downsampling data where there is lot of skewness in few col values within specific groups.
   - **sample_group_by_key**(grouping_cols, sampling_ratio): This api does random sampling of data within each group. Useful for cases where only few groups have highly skewed data and need to be
downsampled.
   - **sample_group_by_max_uniq_values**(grouping_cols, col, max_uniq_values): This api samples data for a specific _col_ but instead of random sampling, takes all the unique values and samples a percentage of the
unique values. Useful for scenarios where all the rows matching the specific column value need to be present.
   - **sample_group_by_max_uniq_values_per_class**(grouping_cols, class_col, col, max_uniq_values_map, _def_max_uniq_values_, _seed_): This api samples different values of _class_col_ differently based on 
_max_uniq_values_map_ map. This is explained in detail in the [sampling documentation].

*Examples*
```
>>> x.sample_class("class", "Iris-setosa", 0.1)
```

### 12. Grouping and Aggregation
   - **aggregate**(grouping_cols, agg_cols, agg_funcs, _collapse_): This is one of the most useful apis for aggregating data based on set of _grouping_cols_, and applying multiple aggregation functions. The _agg_cols_
are the list of columns on which _agg_funcs_ are applied in pairwise manner. 
   - **window_aggregate**(win_col, agg_cols, agg_funcs, winsize, _sliding_, _collapse_): This api is an extension of aggregate where data slices are created using windows of size _winsize_. For each window, _agg_funcs_
are applied on _agg_cols_. If _sliding_ is true, then a sliding window logic is used. Mostly useful for time series data where win_col is date or timestamp, and moving averages are needed.
   - **distinct**(): This api removes all duplicate rows.

*Examples*
Compute total sum of petal_length and petal_width in iris data. Notice the convention in the output columns.
```
>>> x.aggregate("class", ["petal_length", "petal_width"], [sum, sum]).show()

class          	petal_length:sum	petal_width:sum
Iris-setosa    	           73.20	          12.20
Iris-versicolor	          213.00	          66.30
Iris-virginica 	          277.60	         101.30
```

Use **collapse = False** to get all the original rows. Useful for debugging, or chaining multiple aggregate() functions together.
```
>>> x.aggregate("class", ["petal_length", "petal_width"], [sum, sum], collapse = False).show(3)

sepal_length	sepal_width	petal_length	petal_width	class      	petal_length:sum	petal_width:sum
5.10        	       3.50	        1.40	       0.20	Iris-setosa	           73.20	          12.20
4.90        	       3.00	        1.40	       0.20	Iris-setosa	           73.20	          12.20
4.70        	       3.20	        1.30	       0.20	Iris-setosa	           73.20	          12.20
```

### 13. Generic JSON Parsing
   - **explode_json**(url_encoded_col, prefix): This api provides out of the box support for reading simple json blobs and converting to tabular format for data analysis. If there are lists in 
different sections of json, the default merging strategy is similar to cogroup. A more correct way is to use _merge_list_method = join_ where cartisian product will be created. Useful for parsing
json response from web services which are mostly simple in nature, and a default parser can atleast help in looking at the raw data in a simpler way.

A detailed example is provided in [example-notebooks/json-parsing] notebook.

### 14. Join and Union
   - **join**(that, lkeys, _rkeys_, join_type_, _lsuffix_, _rsuffix_, _default_val_, _def_val_map_): This is the primary api for joining two TSV objects. The _lkeys_ is the list of columns on
the left side to use for joining. If the names of join columns in right side are different, the specify the same in _rkeys_. join_type is _inner_, _left_ or _right_. For any outer joins, the
missing values can be either specific at each column in _def_val_map_ or have a fallback global value in _default_val_.
   - **inner_join**(that, lkeys, _rkeys_, _lsuffix_, _rsuffix_, _default_val_, _def_val_map_): This is a wrapper over _join()_ api with _join_type = inner_.
   - **left_join**(that, lkeys, _rkeys_, _lsuffix_, _rsuffix_, _default_val_, _def_val_map_): This is a wrapper over _join()_ api with _join_type = left_.
   - **right_join**(that, lkeys, _rkeys_, _lsuffix_, _rsuffix_, _default_val_, _def_val_map_): This is a wrapper over _join()_ api with _join_type = right_.
   - **union**(tsv_list): This api appends all the TSVs from the tsvlist in the current TSV object. The _tsv_list_ can be a single tsv or an array.

*Examples*
```
>>> low_size = x.le_float("petal_length", 3)
>>> high_size = x.gt_float("petal_length", 3)
>>> low_size.inner_join(high_size, lkeys = "class", lsuffix = "low", rsuffix = "high").select(["class", "petal_length:.*"]).show(3)

class          	sepal_length:low	sepal_width:low	petal_length:low	petal_width:low	sepal_length:high	sepal_width:high	petal_length:high	petal_width:high
Iris-versicolor	             5.1	            2.5	             3.0	            1.1	              7.0	             3.2	              4.7	             1.4
Iris-versicolor	             5.1	            2.5	             3.0	            1.1	              6.4	             3.2	              4.5	             1.5
Iris-versicolor	             5.1	            2.5	             3.0	            1.1	              6.9	             3.1	              4.9	             1.5
```

### 15. Drop and Rename Columns 
   - **drop**(cols): This api deletes the columns from the TSV object. Throws error if any of the column or pattern is missing.
   - **drop_if_exists**(cols): This api deletes the columns from the TSV object. Doesnt throw any error if any of the columns or patterns are missing.
   - **rename**(col, new_col): This api renames the old _col_ as _new_col_. 

*Examples*
Drop columns
```
>>> x.drop("petal.*").show(3)

sepal_length	sepal_width	class      
5.1         	        3.5	Iris-setosa
4.9         	        3.0	Iris-setosa
4.7         	        3.2	Iris-setosa
```

Rename column
```
>>> x.rename("class", "class_label").show(3)

sepal_length	sepal_width	petal_length	petal_width	class_label
5.1         	        3.5	         1.4	        0.2	Iris-setosa
4.9         	        3.0	         1.4	        0.2	Iris-setosa
4.7         	        3.2	         1.3	        0.2	Iris-setosa
```

### 16. Add or Rename Column Prefix and Suffix
   - **rename_suffix**(old_suffix, new_suffix): This api renames all columns that have the suffix _old_suffix_ with the _new_suffix_.
   - **rename_prefix**(old_prefix, new_prefix): This api renames all columns that have the prefix _old_prefix_ with the _new_prefix_.
   - **remove_suffix**(suffix): This api renames the columns having suffix _suffix_ by removing the suffix from their names.
   - **rename_prefix**: This api renames the columns having prefix _prefix_ by removing the prefix from their names.
   - **add_prefix**(prefix, _cols_): This api adds prefix to all the given _cols_. If _cols = None_ then prefix is added to all columns. 
renames the columns having suffix _suffix_ by removing the suffix from their names.
   - **add_prefix**(suffix, _cols_): This api adds suffix to all the given _cols_. If _cols = None_ then prefix is added to all columns. 

*Examples*
```
>>> x.transform_inline("petal.*", lambda x: float(x)*0.4).add_prefix("approx_inches", "petal.*").show(3)

sepal_length	sepal_width	approx_inches:petal_length	approx_inches:petal_width	class      
5.1         	        3.5	                      0.56	                     0.08	Iris-setosa
4.9         	        3.0	                      0.56	                     0.08	Iris-setosa
4.7         	        3.2	                      0.52	                     0.08	Iris-setosa
```

### 17. Sorting
   - **sort**(cols): Sorts the data using the given columns. 
   - **reverse_sort**(cols): This is a wrapper api over _sort()_ for doing sorting in reverse.

*Examples*
```
>>> x.sort("petal_length").show(3)

sepal_length	sepal_width	petal_length	petal_width	class      
4.6         	        3.6	         1.0	        0.2	Iris-setosa
4.3         	        3.0	         1.1	        0.1	Iris-setosa
5.8         	        4.0	         1.2	        0.2	Iris-setosa
```

### 18. Reorder Columns
   - **reorder**(cols): This api reorders the columns in the TSV object for ease of use in jupyter notebooks. In case of multiple columns in _cols_, the original relative ordering is preserved.

*Examples*
```
>>> x.reordre("class").show(3)

class      	sepal_length	sepal_width	petal_length	petal_width
Iris-setosa	         5.1	        3.5	         1.4	        0.2
Iris-setosa	         4.9	        3.0	         1.4	        0.2
Iris-setosa	         4.7	        3.2	         1.3	        0.2
```

### 19. Transpose from Row to Column Format
   - **transpose**(_n_): This api transposes the list of rows and columns. Useful for looking at data with lot of columns that don't fit into the width of the screen.

*Examples*
```
>>> x.transpose(3).show(10)
col_name    	row:1      	row:2      	row:3      
sepal_length	5.1        	4.9        	4.7        
sepal_width 	3.5        	3.0        	3.2        
petal_length	1.4        	1.4        	1.3        
petal_width 	0.2        	0.2        	0.2        
class       	Iris-setosa	Iris-setosa	Iris-setosa
```

### 20. Extending to Other Derived Classes
   - **extend_class**(derived_class, *args, **kwargs): This is an advanced function to plugin extensions and other 3rd party modules. For more details, see [example-notebooks/extend-class-example].

### 21. Conversion to Other Data Formats
   - **to_json_records**(): This api converts each row into a json object of a map. Each row in the output will be a json string.
   - **to_csv**(): This api converts the TSV into simple CSV format file, which means commas(,) and double quotes as special characters are not supported within the fields.
   - **to_df**(): This api converts the TSV object into a pandas dataframe.
   - **to_maps**(): This api converts each row into a map and returns a list of those maps.

*Examples*
Convert each record into json
```
>>> x.to_json_records().show(3, max_col_width = 200)

json
{"sepal_length": "5.1", "sepal_width": "3.5", "petal_length": "1.4", "petal_width": "0.2", "class": "Iris-setosa"}
{"sepal_length": "4.9", "sepal_width": "3.0", "petal_length": "1.4", "petal_width": "0.2", "class": "Iris-setosa"}
{"sepal_length": "4.7", "sepal_width": "3.2", "petal_length": "1.3", "petal_width": "0.2", "class": "Iris-setosa"}
```

### 22. Getting Column Values as Arrays
   - **col_as_array**(col): This api returns all the values of the given _col_ as a string array.
   - **col_as_array_uniq**(col): This api all the unique values of the given _col_ as a string array.
   - **col_as_float_array**(col): This api returns all the values of the given _col_ as float array.
   - **col_as_int_array**(col): This api returns all the values of the given _col_ as int array.

*Examples*
```
>>> x.col_as_array_uniq("class")

['Iris-setosa', 'Iris-versicolor', 'Iris-virginica']

>>> x.col_as_float_array("petal_length")[0:4]

[1.4, 1.4, 1.3, 1.5]
```

### 23. Appending Rows and Columns
   - **add_row**(row_fields): This api adds all the values in the _row_fields_ as column values in the given order to the current TSV.
   - **add_map_as_row**(mp, _default_val_): This api takes all the key values in map _mp_ as column names and values and add to the current TSV. If any column is missing, then _default_val_ is used
to take the default value if it is defined, else throw error.
   - **add_const**(col, value): This api adds a new column _col_ with the given _value_.
   - **add_const_if_missing**(): This api adds a new column _col_ with the given _value_ only if the column is not present already.
   - **add_seq_num**(col): This api assigns a unique sequence number to each row with the name _col_.
   - **replicate_rows**(col): This api reads the value of column _col_, and replicates each row according to the value of the given column.

*Examples*
```
>>> x.add_seq_num("sno").show(3)

sno	sepal_length	sepal_width	petal_length	petal_width	class      
1  	         5.1	        3.5	         1.4	        0.2	Iris-setosa
2  	         4.9	        3.0	         1.4	        0.2	Iris-setosa
3  	         4.7	        3.2	         1.3	        0.2	Iris-setosa
```

### 24. Static Data Transformations
   - **assign_value**(col, value): This api assigns a constant value to an existing column.
   - **copy**(col, new_col): This api copies the column _col_ to _new_col_.
   - **set_missing_values**(cols, default_val): This api sets the value of each column specific in _cols_ as _default_val_ wherever its value is empty.

*Examples*
```
>>> x.assign_value("petal_length", "1.0").show(3)

sepal_length	sepal_width	petal_length	petal_width	class      
5.1         	        3.5	         1.0	        0.2	Iris-setosa
4.9         	        3.0	         1.0	        0.2	Iris-setosa
4.7         	        3.2	         1.0	        0.2	Iris-setosa
```

## C. Visualization, Statistics and Machine Learning
Any functionality that needs extensive 3rd party libraries like matplotlib, seaborn or scikit as provided as extension packages. Not all extension packages might be 
relevant for all users, and please refer to the documentation section of individual packages for further details. Here are some basic explained for visualization.

### Basic Plots
These plots are best described in the [example-notebooks/graphs]. 
   - **linechart**
   - **scatterplot**
   - **histogram**
   - **density**
   - **barchart**
   - **boxplot**

### Advanced Plots
   - **corr_heatmap** 
   - **pairplot**

## D. Enable / Disable Debug Messages
There are certain flags that enable or disable internal log messages. These are meant for advanced users only. 
   - **tsv.enable_debug_mode**(): Enables debug mode.
   - **tsv.disable_debug_mode**(): Disables debug mode.
   - **tsv.set_report_progress_perc**(perc): Displays a progress bar when the operations are finished in multiples of _perc_.
   - **tsv.set_report_progress_min_thresh**(thresh): Sets the debugging only on operations where number of rows is more than _thresh_.

