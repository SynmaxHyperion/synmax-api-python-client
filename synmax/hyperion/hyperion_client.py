import json
import logging
import os

import pandas

from synmax.common import ApiClient, ApiClientAsync, PayloadModelBase

LOGGER = logging.getLogger(__name__)


class ApiPayload(PayloadModelBase):
    def payload(self, pagination_start=None) -> str:
        
        if self.start_date is None:
            payload_start_date = None
        else:
            payload_start_date = str(self.start_date)
        if self.end_date is None:
            payload_end_date = None
        else:
            payload_end_date = str(self.end_date)
        if self.forecast_run_date is None:
            payload_forecast_run_date = None
        else:
            payload_forecast_run_date = str(self.forecast_run_date)
        if type(self.production_month) == int:
            self.production_month = [self.production_month]
        if type(self.state_code) == str:
            self.state_code = [self.state_code]
        if type(self.region) == str:
            self.region = [self.region]
        if type(self.sub_region) == str:
            self.sub_region = [self.sub_region]
        if type(self.county) == str:
            self.county = [self.county]
        if type(self.operator) == str:
            self.operator = [self.operator]
        if type(self.api) == int:
            self.api = [self.api]
        if type(self.aggregate_by) == str:
            self.aggregate_by = [self.aggregate_by]
        if type(self.service_company) == str:
            self.service_company = [self.service_company]
        if type(self.rig_class) == str:
            self.rig_class = [self.rig_class]
        if type(self.completion_class) == str:
            self.completion_class = [self.completion_class]
        if type(self.frac_class) == str:
            self.frac_class = [self.frac_class]    
        if type(self.category) == str:
            self.category = [self.category]
        
        #if type(self.nerc_id) == int:
        #    self.nerc_id = [self.nerc_id]

        _payload = {
            "start_date": payload_start_date,
            "end_date": payload_end_date,
            "forecast_run_date": payload_forecast_run_date,
            "production_month": self.production_month,
            "state_code": self.state_code,
            "region": self.region,
            "sub_region": self.sub_region,
            "county": self.county,
            "operator": self.operator,
            "api": self.api,
            "aggregate_by": self.aggregate_by,
            "service_company": self.service_company,
            "rig_class": self.rig_class,
            "completion_class": self.completion_class,
            "frac_class": self.frac_class,
            "category": self.category,
            #"nerc_id": self.nerc_id,

            "pagination": {
                "start": pagination_start if pagination_start else self.pagination_start
            }
        }
        
        if _payload["production_month"] == None: 
            _payload.pop("production_month")
            
        return json.dumps(_payload)


class HyperionApiClient(object):
    def __init__(self, access_token: str = None, local_server=False, async_client=True):
        """

        :param access_token:
        :param local_server:
        """

        if access_token is None:
            access_token = os.getenv('access_token')
        self.access_key = access_token

        if local_server:
            self._base_uri = 'http://127.0.0.1:8080/'
        else:
            self._base_uri = 'https://hyperion.api.synmax.com/'

        if async_client:
            LOGGER.info('Initializing async client')
            self.api_client = ApiClientAsync(access_token=access_token)
        else:
            self.api_client = ApiClient(access_token=access_token)

        self.api_client_sync = ApiClient(access_token=access_token)

    # GET

    def fetch_regions(self) -> pandas.DataFrame:
        return self.api_client_sync.get(f"{self._base_uri}/v3/regions", return_json=True)

    def fetch_operator_classification(self) -> pandas.DataFrame:
        return self.api_client_sync.get(f"{self._base_uri}/v3/operatorclassification", return_json=True)
    

    # POST
    def daily_fracked_feet(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/dailyfrackedfeet", payload=payload, return_json=True)

    def long_term_forecast(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/longtermforecast", payload=payload, return_json=True)

    def well_completion(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/completions", payload=payload, return_json=True)

    def ducs_by_operator(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/ducsbyoperator", payload=payload, return_json=True)

    def frac_crews(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/fraccrews", payload=payload, return_json=True)

    def production_by_well(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/productionbywell", payload=payload, return_json=True)

    def rigs(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/rigs", payload=payload, return_json=True)

    def wells(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/wells", payload=payload, return_json=True)

    def short_term_forecast(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/shorttermforecast", payload=payload, return_json=True)

    def short_term_forecast_history(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/shorttermforecasthistory", payload=payload, return_json=True)

    def daily_production(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/dailyproduction", payload=payload, return_json=True)
    
    def pipeline_scrapes(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/pipelinescrapes", payload=payload, return_json=True)
    
    
if __name__ == '__main__':
    access_token = 'eyJwcm9qZWN0X2lkIjogIlN5bm1heCBjb21tZXJjaWFsIEFQSSIsICJwcml2YXRlX2tleSI6ICJTd2dHQVVWOEdMdFpibk03WTMzOWIzbnp6VmZYYkFiY09wODlBODN3cE5FIiwgImNsaWVudF9pZCI6ICJGZWxpeCBLZXkiLCAidHlwZSI6ICJvbmVfeWVhcl9saWNlbnNlZF9jdXN0b21lciIsICJzdGFydF9kYXRlIjogIjAzLzEzLzIwMjMiLCAiZW5kX2RhdGUiOiAiMDMvMTMvMjAyNCIsICJ0cmlhbF9saWNlbnNlIjogZmFsc2UsICJpc3N1ZV9kYXRldGltZSI6ICIxMy0wMy0yMDIzIDA3OjQ1OjMwIiwgImFkbWluX3VzZXIiOiBmYWxzZSwgInVzZXJfcm9sZXMiOiBbImh5cGVyaW9uIiwgInZ1bGNhbiJdfQ=='    
    client = HyperionApiClient(access_token=access_token, local_server=True)
    print(client.__dir__())