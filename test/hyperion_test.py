import logging

from synmax.hyperion import HyperionApiClient, ApiPayload

logging.basicConfig(level=logging.DEBUG)

client = HyperionApiClient()


def fetch_region():
    regions = client.fetch_regions()
    print(regions)


def well_completion():
    payload = ApiPayload(start_date='2022-06-1', end_date='2022-06-25', state_code='TX')
    payload.fetch_all = False

    completions = client.wells(payload)
    print(completions)


def main():
    # fetch_region()
    well_completion()


if __name__ == '__main__':
    main()
