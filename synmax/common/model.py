from dataclasses import dataclass


@dataclass
class PayloadModelBase:
    state_code: str
    start_date: str
    end_date: str
    pagination_start: int = 0

    # fetch all data by paginating
    fetch_all = False

    def payload(self):
        # just intercept the payload calls so they aren't relayed to `object`
        pass
