import requests
from typing import Dict
import json
from synmax.common import ApiPayload

def check_payload_has_sufficient_filters(payload:ApiPayload=ApiPayload()) -> bool:
    """Checks if we need to add implicit filters to the payload for query performance
    
    returns has_sufficient_filters: bool """
    
    
    payload_json = json.loads(payload.payload())
    relevant_keys = ["sub_region", "county", "state_code", "region"]
    eval_list = [ payload_json.get(key) for key in relevant_keys]
    if any(eval_list):
        has_sufficient_filters = True
    else:
        has_sufficient_filters = False
    return has_sufficient_filters

def fetch_implicit_filters(target_function:str, filter_type:str, access_key:str) -> Dict:
    url = 'https://hyperion.api.synmax.com/v3/exl_dropdownselection'
    headers = {
        'Access-Key': access_key
    }
    data = {
        "filter_type": filter_type,
        "target_function": target_function
    }

    response = requests.post(url, headers=headers, json=data)
    
    resp_dict = dict({filter_type: response.json()["data"]})

    return resp_dict


def update_payload_with_implicit_filters(filter_value:str, filter_type:str="sub_region", payload:ApiPayload=ApiPayload()) -> ApiPayload:
    """Updates the payload with implicit filters for query performance"""
    payload_json = json.loads(payload.payload())
    if not check_payload_has_sufficient_filters(payload=payload):
        payload_json[filter_type] = [filter_value]
    return ApiPayload(**payload_json)


def reverse_payload_to_user_input(modified_filter_type:str="sub_region", modified_payload:ApiPayload=ApiPayload()) -> ApiPayload:
    """Reverses the payload to state that User inputted"""
    payload_json = json.loads(modified_payload.payload())
    payload_json[modified_filter_type] = None
    return ApiPayload(**payload_json)


if __name__ == "__main__":
    access_token = "eyJwcm9qZWN0X2lkIjogIlN5bm1heCBjb21tZXJjaWFsIEFQSSIsICJwcml2YXRlX2tleSI6ICIwQndzX0ExMFpkdVQyaWlNLS1lbXh3Mk5BNUkxa09kdFNVai04RjVvNzU4IiwgImNsaWVudF9pZCI6ICJGZWxpeCBLZXkiLCAidHlwZSI6ICJvbmVfeWVhcl9saWNlbnNlZF9jdXN0b21lciIsICJzdGFydF9kYXRlIjogIjAzLzE5LzIwMjQiLCAiZW5kX2RhdGUiOiAiMDMvMTkvMjAyNSIsICJ0cmlhbF9saWNlbnNlIjogZmFsc2UsICJpc3N1ZV9kYXRldGltZSI6ICIxOS0wMy0yMDI0IDE0OjI0OjA4IiwgImFkbWluX3VzZXIiOiBmYWxzZSwgInVzZXJfcm9sZXMiOiBbImh5cGVyaW9uIiwgInZ1bGNhbiJdfQ=="
    
    resp = fetch_implicit_filters(target_function="ShortTermForecast",
                     filter_type="sub_region",
                     access_key=access_token)
    
    print(resp)