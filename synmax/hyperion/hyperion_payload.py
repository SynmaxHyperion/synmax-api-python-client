import json
from typing import Optional
from synmax.common import PayloadModelBase

class ApiPayload(PayloadModelBase):
    first_production_month_start: Optional[str] = None
    first_production_month_end: Optional[str] = None
    modeled: Optional[bool] = None

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
        if self.first_production_month_start is None:
            payload_first_production_month_start = None
        else:
            payload_first_production_month_start = str(self.first_production_month_start)
        if self.first_production_month_end is None:
            payload_first_production_month_end = None
        else:
            payload_first_production_month_end = str(self.first_production_month_end)
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
        if type(self.modeled) == bool:
            self.modeled = str(self.modeled)

        # if type(self.nerc_id) == int:
        #    self.nerc_id = [self.nerc_id]

        _payload = {
            "start_date": payload_start_date,
            "end_date": payload_end_date,
            "forecast_run_date": payload_forecast_run_date,
            "first_production_month_start": payload_first_production_month_start,
            "first_production_month_end": payload_first_production_month_end,
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
            "modeled": self.modeled,
            "pagination": {
                "start": pagination_start if pagination_start else self.pagination_start
            }
        }

        if _payload["production_month"] == None:
            _payload.pop("production_month")

        if _payload["modeled"] == None:
            _payload.pop("modeled")

        return json.dumps(_payload)