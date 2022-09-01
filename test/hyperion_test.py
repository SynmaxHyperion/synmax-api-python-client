import logging

from synmax.hyperion import HyperionApiClient, ApiPayload

logging.basicConfig(level=logging.INFO)

client = HyperionApiClient()


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
    payload = ApiPayload(state_code='WY', start_date='2017-01-01', end_date='2017-12-31', production_month=529)
    result_df = client.production_by_well(payload)
    print(result_df.count())


def main():
    # fetch_region()
    # well_completion()
    test_production_by_well()


if __name__ == '__main__':
    main()
