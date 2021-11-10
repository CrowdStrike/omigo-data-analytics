# TSV-DATA-ANALYTICS 

## Utility package to work with TSV files
 * This supports plain or gzip compressed TSV file in local file system or S3
 * Functional programming style API for data processing and analysis

## Build and Install Instructions
```
$ cd python-packages/core
$ python3 -m pip install --upgrade build
$ python3 -m build
$ pip3 install dist/tsv_data_analytics-0.0.1.tar.gz
```

## Usage
```
$ python3
>>> from tsv_data_analytics import tsvutils
>>> x = tsvutils.read_url("https://github.com/owid/covid-19-data/raw/master/public/data/owid-covid-data.csv")
>>> print(x.num_rows())
132224
>>> x.export_to_df(10)
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
>>> y = x \
    .eq_str("iso_code", "USA") \
    .select(["date", "new_cases", "new_deaths"])
>>> y \
    .transform("date", lambda x: x[0:7], "date_month") \
    .set_missing_values(["new_cases", "new_deaths"], "0") \
    .aggregate("date_month", ["new_cases", "new_deaths"], [sum, sum]) \
    .sort("date_month") \
    .show()
date_month	new_cases:sum	new_deaths:sum
2020-01   	            7	             0
2020-02   	           17	             1
2020-03   	       192054	          5358
2020-04   	       888997	         60795
2020-05   	       718602	         41520
2020-06   	       843521	         19691
2020-07   	      1923744	         26463
2020-08   	      1458784	         29555
2020-09   	      1208714	         23497
2020-10   	      1935013	         24635
2020-11   	      4515397	         39312
2020-12   	      6476621	         80927
2021-01   	      6151252	         96652
2021-02   	      2402048	         65786
2021-03   	      1813382	         37263
2021-04   	      1889337	         23892
2021-05   	       918791	         18346
2021-06   	       399850	         10554
2021-07   	      1316775	          8703
2021-08   	      4289478	         27914
2021-09   	      4145088	         58647
2021-10   	      2496549	         46855
2021-11   	       709080	         10925
```
