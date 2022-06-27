# Synmax API Client

## Installation

If you just want to use the package, run:

```bash
pip install --upgrade synmax-api-python-client
```

### Requirements

Make sure you have [Python 3.7+](https://docs.python.org/3/) and [pip](https://pypi.org/project/pip/) installed.

## Quickstart

### Configuring synmax client

```python

import logging
from synmax.hyperion import HyperionApiClient, ApiPayload

# enable debug if required.
logging.basicConfig(level=logging.DEBUG)

# two ways to pass access token.
# 1. Set environment variables: os.environ['access_token'] = 'your token'
# OR
# 2. pass to HyperionApiClient instance
access_token = 'your access token goes here'
hyperion_client = HyperionApiClient(access_token=access_token)

```

#### Fetching data based on your subscription key (access_key)

```python

# fetch regions
regions = hyperion_client.fetch_regions()
print(regions)


```


#### Paginated data

```python

import logging
from synmax.hyperion import HyperionApiClient, ApiPayload

# enable debug if required.
logging.basicConfig(level=logging.DEBUG)

# two ways to pass access token.
# 1. Set environment variables: os.environ['access_token'] = 'your token'
# 2. pass to HyperionApiClient instance
access_token = 'your access token goes here'
client = HyperionApiClient(access_token=access_token)

# well completion based on input filters of type ApiPayload; 
# fetch_all = True will paginate all of rows and return accumulation of each page result
# set fetch_all=False to get first page or any single page starting row with payload.pagination_start = <start row index, default to 0>
payload = ApiPayload(start_date='2022-06-1', end_date='2022-06-25', state_code='TX')
payload.fetch_all = False

completions = client.well_completion(payload)
print(completions)

# output 
# {'data': [{....}, {....}....], 'pagination': {'page_size': 500, 'start': 0, 'total_count': 250}}

```


## publishing package

```shell
pip install twine

python setup.py bdist_wheel 

twine upload dist/*


python setup.py clean --all


```