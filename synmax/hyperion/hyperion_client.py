import json
import logging
import os
from dataclasses import dataclass
import nest_asyncio

import pandas

from synmax.common import ApiClient, ApiClientAsync, PayloadModelBase

LOGGER = logging.getLogger(__name__)
nest_asyncio.apply()


@dataclass
class ApiPayload(PayloadModelBase):
    def payload(self, pagination_start=None) -> str:
        _payload = {
            "start_date": self.start_date,
            "end_date": self.end_date,

            "state_code": self.state_code,
            "region": self.region,
            "sub_region": self.sub_region,
            "operator_name": self.operator_name,
            "production_month": self.production_month,

            "pagination": {
                "start": pagination_start if pagination_start else self.pagination_start
            }
        }
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
        return self.api_client_sync.get(f"{self._base_uri}/regions", return_json=True)

    def fetch_operator_classification(self) -> pandas.DataFrame:
        return self.api_client_sync.get(f"{self._base_uri}/operatorclassification", return_json=True)

    def fetch_long_term_forecast(self) -> pandas.DataFrame:
        return self.api_client_sync.get(f"{self._base_uri}/longtermforecast", return_json=True)

    # POST

    def well_completion(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/completions", payload=payload, return_json=True)

    def ducs_by_operator(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/ducsbyoperator", payload=payload, return_json=True)

    def frac_crews(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/fraccrews", payload=payload, return_json=True)

    def production_by_county_and_operator(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/productionbycountyandoperator", payload=payload,
                                    return_json=True)

    def production_by_well(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/productionbywell", payload=payload, return_json=True)

    def rigs(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/rigs", payload=payload, return_json=True)

    def wells(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/wells", payload=payload, return_json=True)

    def short_term_forecast(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/shorttermforecast", payload=payload, return_json=True)

    def short_term_forecast_history(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/shorttermforecasthistory", payload=payload, return_json=True)

    def daily_production(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/dailyproduction", payload=payload, return_json=True)
