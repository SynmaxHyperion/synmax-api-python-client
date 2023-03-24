import logging
import multiprocessing

import pandas
import pandas as pd
from tqdm import tqdm

from synmax.hyperion import HyperionApiClient, ApiPayload, add_daily, get_fips

logging.basicConfig(level=logging.INFO)

client = HyperionApiClient(local_server=False)


# Test - GET 

def fetch_region():
    regions = client.fetch_regions()
    print(regions)

def fetch_long_term_forecast():
    long_term_forecast = client.fetch_long_term_forecast()
    print(long_term_forecast)

def fetch_operator_classification():
    operator_classification = client.fetch_operator_classification()
    print(operator_classification)

def fetch_daily_fracked_feet():
    daily_fracked_feet = client.fetch_daily_fracked_feet()
    print(daily_fracked_feet)


# Test - POST 

def well_completion():
    payload = ApiPayload(start_date='2021-05-1', end_date='2022-12-25', state_code='CO', operator_name='GREAT WESTERN OPERATING COMPANY LLC')

    # result_df = client.wells(payload)
    result_df = client.well_completion(payload)
    print(result_df.count())


def test_rigs():
    states = ['CO', 'LA', 'ND', 'NM', 'OH', 'OK', 'PA', 'TX', 'WV', 'WY']
    df_list = []
    for _state in states:
        payload = ApiPayload(start_date='2016-01-01', end_date='2022-01-31', state_code=_state)
        result_df = client.rigs(payload)
        print(result_df.count())
        df_list.append(result_df)

    print('Got all stats result')
    if df_list:
        df = pandas.concat(df_list)
        print(df.count())


def test_ducs_by_operator():
    payload = ApiPayload(start_date='2021-01-01', end_date='2021-01-31', operator_name='LIME ROCK RESOURCES LP')

    result_df = client.ducs_by_operator(payload)
    print(result_df.count())


def test_production_by_county_and_operator():
    payload = ApiPayload(start_date='1929-04-01', end_date='1934-01-01', operator_name='Stephens Production Company', state_code='AR')
    result_df = client.production_by_county_and_operator(payload)
    print(result_df.count())


def test_frac_crews():
    payload = ApiPayload(start_date='2021-10-01', end_date='2021-12-31', state_code='CO', sub_region='Colorado wo SJ', region='west', operator_name='BILL BARRETT CORPORATION')

    result_df = client.frac_crews(payload)
    print(result_df.count())


def test_production_by_well():
    # payload = ApiPayload(start_date='2016-01-01', end_date='2016-01-31', production_month=529)
    payload = ApiPayload(state_code='WY', start_date='2017-01-01', end_date='2017-12-31', operator_name='CITATION OIL & GAS CORP', region='west', sub_region='Wyoming')
    # payload = ApiPayload(state_code='LA', start_date='2021-01-01', end_date='2021-01-01', production_month=2)
    # result_df = client.production_by_well(payload)
    # print(result_df.count())
    with multiprocessing.Pool(processes=5) as pool:
        data_list = [payload for _ in range(0, 5)]
        message = 'API query progress'
        list(tqdm(pool.imap(client.production_by_well, data_list), desc=message, total=len(data_list),
                  dynamic_ncols=True, miniters=0))


def test_short_term_forecast():
    # payload = ApiPayload(start_date='2021-08-29', end_date='2022-09-29')
    payload = ApiPayload(start_date='2016-01-01')
    result_df = client.short_term_forecast(payload)
    result_df.to_csv('df_data.csv', index=False)
    print(result_df.count())
    sum_df = result_df[['date', 'gas_monthly']].groupby(by=['date']).sum()
    sum_df.to_csv('sum.csv')


def test_short_term_forecast_history():
    payload = ApiPayload(start_date='2021-08-29', end_date='2021-09-10')
    result_df = client.short_term_forecast_history(payload)
    print(result_df.count())

    payload = ApiPayload(start_date='2020-01-01')
    result_df = client.short_term_forecast(payload)


# TEST HELPERS

def test_daily_func():
    df = pd.DataFrame({'date': ['2022-01-01', '2022-02-01', '2022-03-01'],
                       'gas_monthly': [1000, 2000, 3000],
                       'oil_monthly': [2000, 3000, 4000],
                       'water_monthly': [8000, 9000, 10000]})

    df = add_daily(df)
    print(df)


def test_add_fips():
    df = get_fips()
    print(df)


def compare_df():
    cols = ['api', 'gas_monthly']
    df1 = pandas.read_csv('df_data.csv')
    df2 = pandas.read_csv('GEOML_dbo_v_current_forecast.csv')
    df1 = df1[cols]
    df2 = df2[cols]
    print(df1.count())
    print(df2.count())

    merged_df = pd.merge(df1, df2, how="inner", on=["api"])
    print(merged_df.count())



def main():

    # Test GET
    # fetch_region()
    # fetch_operator_classification()
    # fetch_long_term_forecast()
    # fetch_daily_fracked_feet()

    # Test POST
    #well_completion()
    #test_production_by_well()
    #test_add_fips()
    #test_frac_crews()
    #test_rigs()
    test_short_term_forecast()
    #test_short_term_forecast_history()
    
    #compare_df()


if __name__ == '__main__':

    # enable debug if required.
    """
    logging.basicConfig(level=logging.DEBUG)

    access_token = ''
    
    hyperion_client = HyperionApiClient(access_token=access_token)
    """
    main()