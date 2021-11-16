# TSV-DATA-ANALYTICS 

## Data Analytics package for python
 * Python library to do end to end data analytics from reading to transformation, analysis and visualization.
 * Support for reading and writing multiple data formats from local machine or S3.
 * TSV as the primary data format for data processing.
 * 100+ APIs for simple data transformation and manipulation to get insights.
 * Functional programming style interface for expressing business logic.
 * Advanced APIs for doing join, aggregate, sampling, time window, and json data.
 * Support for ETL kind of data collected periodically over long period of time.
 * APIs designed to work with 100s of columns.
 * Out of the box support for data with changing schema.
 * Visualization APIs to provide simple interface to matplotlib, seaborn, and other popular libraries.

## Build and Install Instructions
There are two packages - core and extensions. The core package is built on core python and out of the box without many dependencies
to keep it stable. The extensions package contains libraries for plotting, and can have lot of dependencies.

To build and install both packages:
```
$ ./install.sh
```

To build the packages separately: 
```
# update the build module
$ python3 -m pip install --upgrade build

# for core package
$ cd python-packages/core
$ python3 -m build
$ pip3 install dist/tsv_data_analytics-0.0.1.tar.gz

# for extensions package
$ cd python-packages/extensions
$ python3 -m build
$ pip3 install dist/tsv_data_analytics_ext-0.0.1.tar.gz
```

## Usage
*Note*: Some working examples are in jupyter ![example-notebooks](example-notebooks) directory. Here is a simple example to run in command line.

#### Import the package to read data
```
$ python3
>>> from tsv_data_analytics import tsvutils
```

#### Read data from public url. Can also use local file or a file in s3
```
>>> x = tsvutils.read("data/iris.tsv.gz")
# other possible options
# x = tsvutils.read("data/iris.tsv")
# x = tsvutils.read("data/iris.tsv.zip")
# x = tsvutils.read("s3://bucket/path_to_file/data.tsv.gz")
# x = tsvutils.read_url("https://github.com/CrowdStrike/tsv-data-analytics/raw/main/data/iris.tsv")
```
#### Print the number of rows
```
>>> print(x.num_rows())
150
```

#### The tsv data can be exported to pandas data frame for general processing
```
>>> x.export_to_df(10)
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
    .select(["sepal_width", "sepal_length"])

>>> y.show(5)

sepal_width	sepal_length
3.5        	         5.1
3.0        	         4.9
3.2        	         4.7
3.1        	         4.6
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
>>> tsvutils.save_to_file(y, "output.tsv.gz")
```
## Notes from the author
* This library is built for simplicity, functionality and robustness. Engineering good practices are followed slowly.
* Contact: amit.jaiswal@gmail.com
