import logging
from typing import List, Dict

import pandas
import requests
from tqdm import tqdm

from synmax.common.model import PayloadModelBase

LOGGER = logging.getLogger(__name__)

_api_timeout = 300


class ApiClient:
    def __init__(self, access_token):
        self.access_key = access_token
        self.session = requests.Session()
        # update headers
        self.session.headers.update(self.headers)

    @property
    def headers(self):
        return {
            'Content-Type': 'application/json',
            'access_key': self.access_key,
            'User-Agent': "Synmax-api-client/1.0.0/python",
        }

    @staticmethod
    def _return_response(response, return_json=False):
        """

        :param response:
        :param return_json:
        :return:
        """
        response.raise_for_status()
        if return_json:
            json_data = response.json()
            if 'error' in json_data:
                raise Exception(json_data['error'])
            return json_data

        return response

    def get(self, url, params=None, return_json=False, **kwargs) -> pandas.DataFrame:
        r"""Sends a GET request.


        :param url: URL for the new :class:`Request` object.
        :param params: (optional) Dictionary, list of tuples or bytes to send
            in the query string for the :class:`Request`.
        :param return_json:
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :return: :class:`Response <Response>` object
        :rtype: requests.Response
        """
        LOGGER.info(url)
        response = self.session.get(url, params=params, timeout=_api_timeout, **kwargs)
        json_result = self._return_response(response, return_json)
        df = pandas.DataFrame(json_result['data'])
        return df

    def post(self, url, payload: PayloadModelBase = None, return_json=False, **kwargs) -> pandas.DataFrame:
        r"""Sends a POST request.

        :param url: URL for the new :class:`Request` object.
        :param payload: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param return_json:
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :return: :class:`Response <Response>` object
        :rtype: requests.Response
        """

        LOGGER.info('Payload data: %s', payload)

        data_list: List[Dict] = []

        response = self.session.post(url, data=payload.payload(), timeout=_api_timeout, **kwargs)
        json_result = self._return_response(response, return_json)
        data_list.extend(json_result['data'])

        pagination = json_result['pagination']
        total_count = pagination['total_count']
        total_pages = pagination['total_count'] // pagination['page_size']
        total_pages = total_pages + 1 if total_count % pagination['page_size'] > 0 else 0

        # first page fetched in the above
        total_pages -= 1

        LOGGER.info('Total data size: %s, total pages to scan: %s', total_count, total_pages)

        with tqdm(desc=F"Querying API {url} pages", total=total_pages, dynamic_ncols=True, miniters=0) as progress_bar:
            while total_count >= pagination['start'] + pagination['page_size']:
                progress_bar.refresh()

                payload.pagination_start = pagination['start'] + pagination['page_size']
                response = self.session.post(url, data=payload.payload(), timeout=_api_timeout, **kwargs)
                json_result = self._return_response(response, return_json)

                pagination = json_result['pagination']
                data_list.extend(json_result['data'])
                progress_bar.update()

        payload.pagination_start = 0
        LOGGER.info('Total response data: %s', len(data_list))
        df = pandas.DataFrame(data_list)
        return df
