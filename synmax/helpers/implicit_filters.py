import requests
from typing import Dict
import json
import sys
sys.path.append(".")
from synmax.hyperion.hyperion_client import ApiPayload

def check_payload_has_sufficient_filters(payload:ApiPayload=ApiPayload()) -> bool:
    """Checks if we need to add implicit filters to the payload for query performance
    
    returns has_sufficient_filters: bool """
    payload_json = json.loads(payload.payload())
    relevant_keys = ["sub_region", "county", "state_code", "region"]
    eval_list = [ payload_json.get(key) for key in relevant_keys]
    
    # if aggregate by is present, this is a valid filter
    if any(eval_list) or payload_json["aggregate_by"] and payload_json["aggregate_by"]:
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