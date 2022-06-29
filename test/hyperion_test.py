import logging

from synmax.hyperion import HyperionApiClient, ApiPayload

logging.basicConfig(level=logging.INFO)

client = HyperionApiClient()


def fetch_region():
    regions = client.fetch_regions()
    print(regions)


def well_completion():
    payload = ApiPayload(start_date='2021-01-1', end_date='2022-06-25', state_code='TX')

    result_df = client.wells(payload)
    print(result_df.count())


def main():
    # fetch_region()
    well_completion()


if __name__ == '__main__':
    main()
