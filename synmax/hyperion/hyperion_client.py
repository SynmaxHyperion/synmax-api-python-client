import json
import logging
import os

import pandas

from synmax.common import ApiClient, ApiClientAsync, PayloadModelBase

LOGGER = logging.getLogger(__name__)


class ApiPayload(PayloadModelBase):
    def payload(self, pagination_start=None) -> str:

        _payload = {
            "start_date": str(self.start_date),
            "end_date": str(self.end_date),
            "forecast_run_date": self.forecast_run_date,
            "production_month": self.production_month,
            "state_code": self.state_code,
            "region": self.region,
            "sub_region": self.sub_region,
            "county": self.county,
            "operator": self.operator,
            "api": self.api,
            "aggregate_by": self.aggregate_by,
            "service_company": self.service_company,
            "nerc_id": self.nerc_id,

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

#    def production_by_county_and_operator(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
#        return self.api_client.post(f"{self._base_uri}/v3/productionbycountyandoperator", payload=payload,
#                                    return_json=True)

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