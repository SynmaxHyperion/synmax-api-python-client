from dataclasses import dataclass


@dataclass
class PayloadModelBase:
    pagination_start: int = 0
    start_date: str = None
    end_date: str = None

    production_month: int = None
    state_code: str = None
    region: str = None
    sub_region: str = None
    operator: str = None
    api: list = None
    aggregate_by: str = None
    forecast_run_date: str = None

    def payload(self, pagination_start=None):
        # just intercept the payload calls so they aren't relayed to `object`
        pass
