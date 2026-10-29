# omigo-hydra

Distributed execution for the [Data Analytics Library for
Python](https://github.com/CrowdStrike/omigo-data-analytics).

This package contains experimental code for running analytics across a cluster,
along with the file path readers and filesystem wrappers that support it. It is
experimental and not used in production at the moment.

## Install

```
pip3 install omigo-core omigo-ext omigo-hydra --upgrade
```

## Usage

```python
from omigo_hydra import hydra

x = hydra.read("data/iris.tsv.gz")
```

See the [project
README](https://github.com/CrowdStrike/omigo-data-analytics#readme) for full
documentation.
