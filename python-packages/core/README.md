# omigo-core

Core package of the [Data Analytics Library for
Python](https://github.com/CrowdStrike/omigo-data-analytics) — end to end data
analytics from reading through transformation and analysis, with functional
programming style APIs.

This package is built on core Python with minimal external dependencies, to keep
it stable. It provides the `dataframe` and `tsv` data structures, readers and
writers for the local filesystem, S3 and web URLs, joins, aggregation, sampling,
time series helpers and schema evolution.

## Install

```
pip3 install omigo-core --upgrade
```

## Usage

```python
from omigo_core import dataframe
```

See the [project
README](https://github.com/CrowdStrike/omigo-data-analytics#readme) for full
documentation, and the [example
notebooks](https://github.com/CrowdStrike/omigo-data-analytics/tree/main/example-notebooks)
for worked examples.
