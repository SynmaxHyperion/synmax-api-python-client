import calendar
import os

import pandas as pd

from .hyperion_client import HyperionApiClient, ApiPayload


def monthly_to_daily(row, prod_column='gas_monthly', date_column='date'):
    """

    :param row:
    :param prod_column:
    :param date_column:
    :return:
    """
    NumberTypes = (int, float)
    if isinstance(row[prod_column], NumberTypes):
        date_ = pd.to_datetime(row[date_column])
        days = calendar.monthrange(date_.year, date_.month)[1]

        return row[prod_column] / days
    return 0


def add_daily(df: pd.DataFrame, date_column='date', monthly_columns=['gas_monthly', 'oil_monthly', 'water_monthly'],
              daily_columns=['gas_daily', 'oil_daily', 'water_daily']):
    """
    Used to add daily production columns to a Pandas dataframe containing monthly production columns.
    :param df: A Pandas dataframe containing monthly columns of production and a date column
    :param date_column: string containing the name of the column denoting the date
    :param monthly_columns: a list of strings contaning the monthly production columns
    :param daily_columns: a list of strings containing the desired names of the outputed daily columns
    which correspond positionaly to the monthly_columns
    :return: a Pandas dataframe
    """
    for index, column in enumerate(monthly_columns):
        if column in df.columns:
            df[daily_columns[index]] = df.apply(monthly_to_daily, axis=1, args=(column, date_column,))
        else:
            print(f'Skipping {column} which does not exists')

    return df


def get_fips():
    """
    Returns lookup table for FIPS codes
    :return: Pandas dataframe
    """
    from synmax.config import DATA_FOLDER
    return pd.read_csv(os.path.join(DATA_FOLDER, 'fips_lookup.csv'))


def make_fips():
    fips_df = pd.read_csv(
        'https://raw.githubusercontent.com/kjhealy/fips-codes/master/state_and_county_fips_master.csv')
    fips_df['name'] = fips_df.apply(lambda x: x['name'].replace(' County', '').replace(' Parish', ''), axis=1)
    fips_df.columns = ['fips', 'county', 'state_ab']
    fips_df['county'] = fips_df.county.str.upper()

    fips_df.to_csv('fips_lookup.csv', index=False)
