# TSV-DATA-ANALYTICS 

## Data analytics library for python
 * Python library to do end to end data analytics from reading to transformation, analysis and visualization.
 * Support for reading and writing multiple data formats from local machine or S3.
 * TSV as the primary data format for data processing.
 * 100+ APIs for simple data transformation to get insights.
 * Functional programming style interface for expressing business logic.
 * Advanced APIs for doing join, aggregate, sampling, time window, and json data.
 * Support for ETL kind of data collected periodically over long period of time.
 * APIs designed to work with 100s of columns.
 * Out of the box support for data with changing schema.
 * Visualization APIs to provide simple interface to matplotlib, seaborn, and other popular libraries.

## Install Instructions
There are two packages - core and extensions. The core package is built on core python and out of the box without many dependencies
to keep it stable. The extensions package contains libraries for plotting, and can have lot of dependencies.

```
$ pip3 install tsv-data-analytics==0.0.5
$ pip3 install tsv-data-analytics-ext==0.0.5
```

## Usage
*Note*: Some working examples are in jupyter ![example-notebooks](example-notebooks) directory. Here is a simple example to run in command line.

#### Read data from local filesystem. Can also use s3 or web url.
```
$ python3
>>> from tsv_data_analytics import tsv
>>> x = tsv.read("data/iris.tsv.gz")
#
# other possible options
#
# x = tsv.read("data/iris.tsv")
# x = tsv.read("data/iris.tsv.zip")
# x = tsv.read("s3://bucket/path_to_file/data.tsv.gz")
# x = tsv.read("https://github.com/CrowdStrike/tsv-data-analytics/raw/main/data/iris.tsv")
```
#### Print basic stats like the number of rows
```
>>> print(x.num_rows())
150
```

#### Export to pandas data frame for nice display, or use any of pandas apis. 
```
>>> x.to_df(10)
  sepal_length sepal_width petal_length petal_width        class
0          5.1         3.5          1.4         0.2  Iris-setosa
1          4.9         3.0          1.4         0.2  Iris-setosa
2          4.7         3.2          1.3         0.2  Iris-setosa
3          4.6         3.1          1.5         0.2  Iris-setosa
4          5.0         3.6          1.4         0.2  Iris-setosa
5          5.4         3.9          1.7         0.4  Iris-setosa
6          4.6         3.4          1.4         0.3  Iris-setosa
7          5.0         3.4          1.5         0.2  Iris-setosa
8          4.4         2.9          1.4         0.2  Iris-setosa
9          4.9         3.1          1.5         0.1  Iris-setosa
```

#### Example of filtering data for specific column value and select specific columns
```
>>> y = x \
    .eq_str("class", "Iris-setosa") \
    .gt("sepal_width", 3.1) \
    .select(["sepal_width", "sepal_length"])

>>> y.show(5)

sepal_width	sepal_length
3.5        	         5.1
3.2        	         4.7
3.6        	         5.0
```
#### Import the graph extension package for creating charts
```
>>> from tsv_data_analytics_ext import graphext
>>> x.extend_class(graphext.VisualTSV).histogram("sepal_length", "class", yfigsize = 8)
```
![iris sepal_width histogram](images/iris-hist.png)

#### Some of the more advanced graphs are also available
```
>>> x.extend_class(graphext.VisualTSV).pairplot(["sepal_length", "sepal_width"], kind = "kde", diag_kind = "auto")
```
![iris sepal_width pairplot](images/iris-pairplot.png)

#### The tsv file can be saved to local file system or s3
```
>>> tsv.write(y, "output.tsv.gz")
```
## Notes from the author
* This library is built for simplicity, functionality and robustness. Engineering good practices are followed slowly.
* Detailed documentation is currently in progress. Feel free to reach out for any questions or help.
