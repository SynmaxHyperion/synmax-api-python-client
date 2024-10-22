import json
import logging
import os
from typing import Optional
import tqdm
import pandas

from synmax.common import ApiClient, ApiClientAsync
from synmax.hyperion.hyperion_payload import ApiPayload
from synmax.helpers.implicit_filters import check_payload_has_sufficient_filters, \
                                            fetch_implicit_filters, \
                                            update_payload_with_implicit_filters, \
                                            reverse_payload_to_user_input
LOGGER = logging.getLogger(__name__)

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

        async_client = False  # do not run in async to avoid http 429 throttle limit
        if async_client:
            LOGGER.info('Initializing async client')
            self.api_client = ApiClientAsync(access_token=access_token)
        else:
            self.api_client = ApiClient(access_token=access_token)

        self.api_client_sync = ApiClient(access_token=access_token)

    # GET

    def fetch_regions(self) -> pandas.DataFrame:
        return self.api_client_sync.get(f"{self._base_uri}/v3/regions", return_json=True)
    
    def fetch_dtils(self) -> pandas.DataFrame:
        return self.api_client_sync.get(f"{self._base_uri}/v3/dtils", return_json=True)

    def fetch_operator_classification(self) -> pandas.DataFrame:
        return self.api_client_sync.get(f"{self._base_uri}/v3/operatorclassification", return_json=True)

    def fetch_pipeline_scrape_status(self) -> pandas.DataFrame:
        return self.api_client_sync.get(f"{self._base_uri}/v3/pipelinescrapestatus", return_json=True)
    
    def fetch_til_monitoring(self) -> pandas.DataFrame:
        return self.api_client_sync.get(f"{self._base_uri}/v3/til_monitoring", return_json=True)
    
    def fetch_forecast_run_dates(self) -> pandas.DataFrame:
        return self.api_client_sync.get(f"{self._base_uri}/v3/shorttermforecasthistorydates", return_json=True)

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
        payload_has_suff_filters = check_payload_has_sufficient_filters()
        if payload_has_suff_filters:
            return self.api_client.post(f"{self._base_uri}/v3/shorttermforecast", payload=payload, return_json=True)
        else:
            implicit_filter_type = "sub_region"
            implicit_filters_dict = fetch_implicit_filters(target_function="ShortTermForecast",
                                                                filter_type=implicit_filter_type,
                                                                access_key=self.access_key)

            df_list = []
            for filter_value in tqdm.tqdm(implicit_filters_dict[implicit_filter_type]):
                payload = update_payload_with_implicit_filters(filter_value=filter_value,
                                                                filter_type=implicit_filter_type,
                                                                payload=payload)
                
                df_filter_value = self.api_client.post(f"{self._base_uri}/v3/shorttermforecast",
                                            payload=payload,
                                            return_json=True)
                
                df_list.append(df_filter_value)
                
                payload = reverse_payload_to_user_input(modified_filter_type=implicit_filter_type,
                                            modified_payload=payload)
            df_result_combined = pandas.concat(df_list)
            return df_result_combined

    def short_term_forecast_history(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/shorttermforecasthistory", payload=payload, return_json=True)

    def short_term_forecast_declines(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/shorttermforecastdeclines", payload=payload, return_json=True)

    def daily_production(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/dailyproduction", payload=payload, return_json=True)

    def pipeline_scrapes(self, payload: ApiPayload = ApiPayload()) -> pandas.DataFrame:
        return self.api_client.post(f"{self._base_uri}/v3/pipelinescrapes", payload=payload, return_json=True)
