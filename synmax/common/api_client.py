import logging
from typing import List, Dict

import requests

from synmax.common.model import PayloadModelBase

LOGGER = logging.getLogger(__name__)


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

    def get(self, url, params=None, return_json=False, **kwargs) -> Dict:
        r"""Sends a GET request.


        :param url: URL for the new :class:`Request` object.
        :param params: (optional) Dictionary, list of tuples or bytes to send
            in the query string for the :class:`Request`.
        :param return_json:
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :return: :class:`Response <Response>` object
        :rtype: requests.Response
        """
        LOGGER.debug(url)
        response = self.session.get(url, params=params, **kwargs)
        return self._return_response(response, return_json)

    def post(self, url, payload: PayloadModelBase = None, return_json=False, **kwargs) -> Dict:
        r"""Sends a POST request.

        :param url: URL for the new :class:`Request` object.
        :param payload: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param return_json:
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :return: :class:`Response <Response>` object
        :rtype: requests.Response
        """

        data_list: List[Dict] = []

        while True:
            LOGGER.debug('Payload data: %s', payload)
            LOGGER.debug('Calling api: %s', url)
            response = self.session.post(url, data=payload.payload(), **kwargs)
            json_result = self._return_response(response, return_json)
            pagination = json_result['pagination']
            data_list.extend(json_result['data'])
            if not payload.fetch_all or pagination['total_count'] <= pagination['start'] + pagination['page_size']:
                break

            payload.pagination_start = pagination['start'] + pagination['page_size']

        LOGGER.debug('Total response data: %s', len(data_list))

        return {'data': data_list, 'pagination': pagination}
