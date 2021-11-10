# TSV-DATA-ANALYTICS 

## Utility package to work with TSV files
 * This supports plain or gzip compressed TSV file in local file system or S3
 * Functional programming style API for data processing and analysis

## Build and Install Instructions
```
# for core package
$ cd python-packages/core
$ python3 -m pip install --upgrade build
$ python3 -m build
$ pip3 install dist/tsv_data_analytics-0.0.1.tar.gz

# for extensions package
$ cd python-packages/extensions
$ python3 -m pip install --upgrade build
$ python3 -m build
$ pip3 install dist/tsv_data_analytics_ext-0.0.1.tar.gz
```

## Usage
```
Some working examples are in jupyter notebooks directory. Here is a simple example to run in command line.

$ python3
# import the packages
from tsv_data_analytics import tsvutils

# read data from public url. Can also use local file or a file in s3
x = tsvutils.read_url("https://github.com/owid/covid-19-data/raw/master/public/data/owid-covid-data.csv")

# print the number of rows
print(x.num_rows())
132224

# the tsv data can be exported to pandas data frame for general processing
x.export_to_df(10)
  iso_code continent     location        date total_cases  ... human_development_index excess_mortality_cumulative_absolute excess_mortality_cumulative excess_mortality excess_mortality_cumulative_per_million
0      AFG      Asia  Afghanistan  2020-02-24         5.0  ...                   0.511                                                                                                                          
1      AFG      Asia  Afghanistan  2020-02-25         5.0  ...                   0.511                                                                                                                          
2      AFG      Asia  Afghanistan  2020-02-26         5.0  ...                   0.511                                                                                                                          
3      AFG      Asia  Afghanistan  2020-02-27         5.0  ...                   0.511                                                                                                                          
4      AFG      Asia  Afghanistan  2020-02-28         5.0  ...                   0.511                                                                                                                          
5      AFG      Asia  Afghanistan  2020-02-29         5.0  ...                   0.511                                                                                                                          
6      AFG      Asia  Afghanistan  2020-03-01         5.0  ...                   0.511                                                                                                                          
7      AFG      Asia  Afghanistan  2020-03-02         5.0  ...                   0.511                                                                                                                          
8      AFG      Asia  Afghanistan  2020-03-03         5.0  ...                   0.511                                                                                                                          
9      AFG      Asia  Afghanistan  2020-03-04         5.0  ...                   0.511                                                   

# simple example of filtering data for USA and selecting specific columns
y = x \
    .eq_str("iso_code", "USA") \
    .select(["date", "new_cases", "new_deaths"])

# example of filling missing values in data and running aggregate to compute monthly cases and deaths
z = y \
    .transform("date", lambda x: x[0:7], "date_month") \
    .set_missing_values(["new_cases", "new_deaths"], "0") \
    .aggregate("date_month", ["new_cases", "new_deaths"], [sum, sum]) \
    .sort("date_month")

z.show(5)

date_month	new_cases:sum	new_deaths:sum
2020-01   	            7	             0
2020-02   	           17	             1
2020-03   	       192054	          5358
2020-04   	       888997	         60795
2020-05   	       718602	         41520

# the tsv data can be saved easily to local file system or s3
tsvutils.save_to_file(z, "output.tsv.gz")
```
