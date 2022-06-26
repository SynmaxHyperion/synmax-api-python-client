import json
import os
import logging
from dataclasses import dataclass

from synmax.common import ApiClient, PayloadModelBase

LOGGER = logging.getLogger(__name__)

_API_BASE = 'https://hyperion-api-xzzxclvs3q-uc.a.run.app'


# _API_BASE = 'https://fracmodel.synmax.com'


@dataclass
class ApiPayload(PayloadModelBase):
    def payload(self) -> str:
        _payload = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "state_code": self.state_code,
            "pagination": {
                "start": self.pagination_start
            }
        }
        return json.dumps(_payload)


class HyperionApiClient(object):
    def __init__(self, access_token: str = None):
        if access_token is None:
            access_token = os.getenv('access_token')
        self.access_key = access_token
        self.api_client = ApiClient(access_token=access_token)

    def fetch_regions(self):
        return self.api_client.get(f"{_API_BASE}/regions", return_json=True)

    def well_completion(self, payload: ApiPayload):
        return self.api_client.post(f"{_API_BASE}/completions", payload=payload, return_json=True)

    def ducs_by_operator(self, payload: ApiPayload):
        return self.api_client.post(f"{_API_BASE}/ducsbyoperator", payload=payload, return_json=True)

    def frac_crews(self, payload: ApiPayload):
        return self.api_client.post(f"{_API_BASE}/fraccrews", payload=payload, return_json=True)

    def production_by_county_and_operator(self, payload: ApiPayload):
        return self.api_client.post(f"{_API_BASE}/productionbycountyandoperator", payload=payload, return_json=True)

    def production_by_well(self, payload: ApiPayload):
        return self.api_client.post(f"{_API_BASE}/productionbywell", payload=payload, return_json=True)

    def rigs(self, payload: ApiPayload):
        return self.api_client.post(f"{_API_BASE}/rigs", payload=payload, return_json=True)

    def wells(self, payload: ApiPayload):
        return self.api_client.post(f"{_API_BASE}/wells", payload=payload, return_json=True)
