import logging

import pandas as pd

from synmax.hyperion import HyperionApiClient, ApiPayload, add_daily, get_fips

logging.basicConfig(level=logging.INFO)

client = HyperionApiClient(local_server=False)


def fetch_region():
    regions = client.fetch_regions()
    print(regions)


def well_completion():
    payload = ApiPayload(start_date='2021-05-1', end_date='2022-06-25', state_code='TX')

    result_df = client.wells(payload)
    print(result_df.count())


def test_ducs_by_operator():
    payload = ApiPayload(start_date='2021-01-01', end_date='2021-01-31')

    result_df = client.ducs_by_operator(payload)
    print(result_df.count())


def test_production_by_county_and_operator():
    payload = ApiPayload(start_date='2018-01-01', end_date='2018-01-31')
    result_df = client.production_by_county_and_operator(payload)
    print(result_df.count())


def test_production_by_well():
    # payload = ApiPayload(start_date='2016-01-01', end_date='2016-01-31', production_month=529)
    # payload = ApiPayload(state_code='WY', start_date='2017-01-01', end_date='2017-12-31', production_month=529)
    payload = ApiPayload(state_code='LA', start_date='2021-01-01', end_date='2021-01-01', production_month=2)
    payload = ApiPayload(state_code='LA', start_date='2021-01-01', end_date='2021-01-01', production_month=2)
    result_df = client.production_by_well(payload)
    print(result_df.count())

def test_daily_func():
    df = pd.DataFrame({'date': ['2022-01-01', '2022-02-01', '2022-03-01'],
                       'gas_monthly': [1000, 2000, 3000],
                       'oil_monthly': [2000, 3000, 4000],
                       'water_monthly': [8000, 9000, 10000]})

    df = add_daily(df)
    print(df)

def test_add_fips():
    df = pd.DataFrame({'date': ['2022-01-01', '2022-02-01', '2022-03-01'],
                       'state_ab': ['TX', 'NM', 'LA'],
                       'county': ['Midland', 'Lea', 'De Soto']})
    df = get_fips()
    print(df)

def main():
    # fetch_region()
    # well_completion()
    test_production_by_well()


if __name__ == '__main__':
    test_add_fips()
