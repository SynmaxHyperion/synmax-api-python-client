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
from synmax.hyperion import HyperionApiClient, ApiPayload

hyperion_client = HyperionApiClient(access_token='....')
# fetch regions
region_df = hyperion_client.fetch_regions()
print(region_df.count())


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
hyperion_client = HyperionApiClient(access_token=access_token)

# well completion based on input filters of type ApiPayload; 
# fetch_all = True will paginate all of rows and return accumulation of each page result. True by default
# set fetch_all=False to get first page or any single page starting row with payload.pagination_start = <start row index, default to 0>
payload = ApiPayload(start_date='2022-06-1', end_date='2022-06-25', state_code='TX')

# return result is in pandas.DataFrame
completions_df = hyperion_client.well_completion(payload)
print(completions_df.count())
# output is in pandas.DataFrame
# Querying API pages: 100%|██████████| 8/8 [00:06<00:00,  1.14it/s]

# with optional payload to fetch full dataset
result_df = hyperion_client.wells()
print(result_df.count())
# Querying API wells pages:   0%|          | 4/7225 [00:16<8:51:17,  4.41s/it]


## Well data
result_df = hyperion_client.wells(payload)

## Product by Country and Operator
result_df = hyperion_client.production_by_county_and_operator(payload)

## Available api methods on hyperion_client
dir(hyperion_client)
# output: ['ducs_by_operator', 'fetch_regions', 'frac_crews', 'production_by_county_and_operator', 'production_by_well', 'rigs', 'well_completion', 'wells']

```

## publishing package

```shell
pip install twine

python setup.py bdist_wheel 

twine upload dist/*


python setup.py clean --all


```