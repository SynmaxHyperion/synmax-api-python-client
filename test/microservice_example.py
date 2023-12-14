import json
import os
import logging
from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import date
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


class PayloadModelBase(BaseModel):
    pagination_start: Optional[int] = 0
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    forecast_run_date: Optional[str] = None
    production_month: Optional[Union[int, List[int]]] = None
    state_code: Optional[Union[str, List[str]]] = None
    region: Optional[Union[str, List[str]]] = None
    sub_region: Optional[Union[str, List[str]]] = None
    county: Optional[Union[str, List[str]]] = None
    operator: Optional[Union[str, List[str]]] = None
    api: Optional[Union[int, List[int]]] = None
    aggregate_by: Optional[Union[str, List[str]]] = None
    service_company: Optional[Union[str, List[str]]] = None
    frac_class: Optional[Union[str, List[str]]] = None
    rig_class: Optional[Union[str, List[str]]] = None
    completion_class: Optional[Union[str, List[str]]] = None
    # nerc_id: Optional[Union[str, List[str]]] = None

    def payload(self, pagination_start=None):
        # just intercept the payload calls so they aren't relayed to `object`
        pass


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
            #"nerc_id": self.nerc_id,

            "pagination": {
                "start": pagination_start if pagination_start else self.pagination_start
            }
        }
        
        if _payload["production_month"] == None: 
            _payload.pop("production_month")
            
        return json.dumps(_payload)



class ApiRequestClient: 
    def __init__(self, logger, api_key, api_url='https://hyperion.api.synmax.com/'):
        self.logger = logger
        self.access_key = api_key
        self.api_url = api_url
        self.headers = {
            'Content-Type': 'application/json',
            'access_key': self.access_key,
            'User-Agent': "Synmax-api-client/1.0.1/python",
        }

    def _get(self, endpoint, payload):
        self.logger.info('GET {}'.format(endpoint))
        self.logger.info('payload: {}'.format(payload))
        response = requests.get(
            url=self.api_url + endpoint,
            headers=self.headers,
            data=payload.payload()
        )
        if response.status_code == 200:
            return response.json()
        else:
            self.logger.error('Error: {}'.format(response.status_code))
            self.logger.error(response.json())
            return None

    def _post(self, endpoint, payload):
        self.logger.info('POST {}'.format(endpoint))
        self.logger.info('payload: {}'.format(payload))
        print("Attempting to post", self.api_url + endpoint)
        
        response = requests.post(
            url=self.api_url + endpoint,
            headers=self.headers,
            data=json.dumps(payload.__dict__)
        )
        if response.status_code == 200:
            return response.json()
        else:
            self.logger.error('Error: {}'.format(response.status_code))
            self.logger.error(response.json())
            return None
        
        
if __name__ == '__main__':
    
    api_key = 'YOUR API KEY' ## # os.environ.get('SYNMAX_API_KEY')
    client = ApiRequestClient(logger=logger, api_key=api_key)
    
    # EXAMPLE 1:   Synmax_energy_frontend_node/app/src/controllers/geoMl.controller.ts - Line 108
    
    # Query: 
    # SELECT date, COUNT(status) FROM customer_api_timeseries where status = 'fracking' group by date order by date desc
    
    # Translate to API Payload:
    payload = ApiPayload(aggregate_by='date')
    
    json_result = client._post(endpoint='v3/fraccrews', payload=payload)
    
    print(json_result)
    
    # OTHER EXAMPLES: 
    
    #payload = ApiPayload(start_date='2022-05-01',
    #                     end_date='2022-07-25', 
    #                     sub_region=['Haynesville - TX', 'Haynesville - LA'],
    #                     operator=['BLUE DOME OPERATING LLC', 'ENSIGHT ENERGY MANAGEMENT LLC'],
    #                     aggregate_by=['operator']
    #                     )

