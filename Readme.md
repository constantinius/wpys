# WPyS - Web Processing Service with Python

"Simple" server to implement WPS 2.0. This is an experimental project, mainly
for my own learning/experimenting purposes. Maybe, at some point, this will
be turned into a production, but right now it is far from.

Requires Python 3.7 (maybe lower, if you install `dataclasses` separetely)

## Installation


```bash
pip install quart
```

## Running

```bash
# run the server
WPYS_CONFIG_FILE=wpys_examples/config.yaml QUART_APP=wpys.server:app quart run

# run the worker
WPYS_CONFIG_FILE=wpys_examples/config.yaml python3 -m wpys.worker
```
