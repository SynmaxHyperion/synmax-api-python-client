from dataclasses import dataclass


@dataclass
class PayloadModelBase:
    pagination_start: int = 0
    start_date: str = None
    end_date: str = None

    production_month: int = 0
    state_code: str = None
    region: str = None
    sub_region: str = None
    operator_name: str = None

    # fetch_all = True

    def payload(self, pagination_start=None):
        # just intercept the payload calls so they aren't relayed to `object`
        pass
