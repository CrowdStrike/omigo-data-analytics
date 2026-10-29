# omigo-ext

Extensions for [`omigo-core`](https://pypi.org/project/omigo-core/), part of the
[Data Analytics Library for
Python](https://github.com/CrowdStrike/omigo-data-analytics).

Where core deliberately keeps its dependencies minimal, this package holds the
advanced functionality and therefore carries many more of them: visualization
via matplotlib, seaborn, plotly and bokeh, graph rendering via graphviz, and
connectors for Kafka, Splunk, LogScale, Presto, Spark, Jira and the Google API
client.

There are APIs for creating new extension packages for custom needs and plugging
them into the existing code — see
[extend-class](https://github.com/CrowdStrike/omigo-data-analytics/blob/main/example-notebooks/extend-class.ipynb).

## Install

```
pip3 install omigo-core omigo-ext --upgrade
```

See the [project
README](https://github.com/CrowdStrike/omigo-data-analytics#readme) for full
documentation.
