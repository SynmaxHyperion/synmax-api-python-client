from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import date


class PayloadModelBase(BaseModel):
    pagination_start: Optional[int] = 0
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    forecast_run_date: Optional[date] = None
    production_month: Optional[Union[int, List[int]]] = None
    state_code: Optional[Union[str, List[str]]] = None
    region: Optional[Union[str, List[str]]] = None
    sub_region: Optional[Union[str, List[str]]] = None
    county: Optional[Union[str, List[str]]] = None
    operator: Optional[Union[str, List[str]]] = None
    api: Optional[Union[int, List[int]]] = None
    aggregate_by: Optional[Union[str, List[str]]] = None
    service_company: Optional[Union[str, List[str]]] = None
    # nerc_id: Optional[Union[str, List[str]]] = None

    def payload(self, pagination_start=None):
        # just intercept the payload calls so they aren't relayed to `object`
        pass
