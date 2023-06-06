from pydantic import BaseModel
from typing import Optional, List
from datetime import date


class PayloadModelBase(BaseModel):
    pagination_start: Optional[int] = 0
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    forecast_run_date: Optional[date] = None
    production_month: Optional[List[int]] = None
    state_code: Optional[List[str]] = None
    region: Optional[List[str]] = None
    sub_region: Optional[List[str]] = None
    county: Optional[List[str]] = None
    operator: Optional[List[str]] = None
    api: Optional[List[int]] = None
    aggregate_by: Optional[List[str]] = None
    service_company: Optional[List[str]] = None
    nerc_id: Optional[List[str]] = None

    def payload(self, pagination_start=None):
        # just intercept the payload calls so they aren't relayed to `object`
        pass
