"""Install SynMax Python Client"""

# pip install --upgrade synmax-api-python-client

"""Install other Dependencies"""

# pip install plotly-geo plotly geopandas==0.8.1 pyshp shapely

"""Query the production data"""

from synmax.hyperion import HyperionApiClient, ApiPayload, add_daily, get_fips
import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff

access_token = '************************************'
client = HyperionApiClient(access_token=access_token)

payload = ApiPayload(start_date='2021-01-01', end_date='2022-09-01', state_code='TX', production_month=2)
df = client.production_by_well(payload)
# df.to_csv(r'C:\Users\eric\Desktop\TEMP\prod_ip.csv', index=False)
# df = pd.read_csv(r'C:\Users\eric\Desktop\TEMP\prod_ip.csv')

"""convert to mcf/d"""
df = add_daily(df)

"""examine county-level histograms"""

fig = px.histogram(df[df.county=='Midland'], x='gas_daily')
fig.show()

"""calculate average per county"""

county_df = df[['state_ab', 'county', 'gas_daily']].groupby(['state_ab', 'county'], as_index=False).mean()

"""Get FIPS code Lookup Table"""

fips_df = get_fips()

"""merge fips_df with county_df"""
county_df = county_df.merge(fips_df[fips_df.state_ab == 'TX'], how='right', on=['state_ab','county'])
county_df['gas_daily'] = county_df['gas_daily'].fillna(0)

"""Create county map"""

fig = ff.create_choropleth(fips=county_df.fips.tolist(),
                           values=county_df.gas_daily.tolist(),
                           county_outline={'color': 'rgb(255,255,255)', 'width': 0.5},
                           scope=['TX'],
                           binning_endpoints=[75, 125, 250, 500, 1000, 2000, 4000, 8000, 16000, 32000])
fig.show()