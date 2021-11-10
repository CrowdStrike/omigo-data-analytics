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
>>> x = tsvutils.read("/path/to/file.tsv.gz")
>>> x.sample_rows(10).show()
```
