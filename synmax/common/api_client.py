import asyncio
import logging
from typing import List, Dict

import aiohttp
import pandas
import requests
from aioretry import (
    retry,
    # Tuple[bool, Union[int, float]]
    RetryPolicyStrategy,
    RetryInfo
)
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3 import Retry

from synmax.common.model import PayloadModelBase

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

_api_timeout = 600
PARALLEL_REQUESTS = 25


class ApiClientBase:
    def __init__(self, access_token):
        self.access_key = access_token
        self.session = requests.Session()
        self.session.verify = False
        # update headers
        self.session.headers.update(self.headers)

        # HTTPAdapter
        retry_strategy = Retry(
            total=10,
            backoff_factor=2,
            status_forcelist=[408, 429, 500, 502, 503, 504, 505],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"],

        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    @property
    def headers(self):
        return {
            'Content-Type': 'application/json',
            'access_key': self.access_key,
            'User-Agent': "Synmax-api-client/1.0.1/python",
        }

    @staticmethod
    def _return_response(response, return_json=False):
        """

        :param response:
        :param return_json:
        :return:
        """
        # response.raise_for_status()
        if not response.ok:
            # logging.error('Error in response. %s')
            return None

        if return_json:
            json_data = response.json()
            if 'error' in json_data:
                # raise Exception(json_data['error'])
                logging.error(json_data['error'])
                return None
            return json_data

        return response


class ApiClient(ApiClientBase):

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

        if json_result:
            df = pandas.DataFrame(json_result['data'])
            return df

        return None

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
        got_first_page = False
        total_count = -1

        with tqdm(desc=F"Querying API {url} pages", total=1, dynamic_ncols=True, miniters=0) as progress_bar:
            while not got_first_page or total_count >= pagination['start'] + pagination['page_size']:
                try:
                    progress_bar.refresh()
                    response = self.session.post(url, data=payload.payload(), timeout=_api_timeout, **kwargs)
                    if response.status_code == 401:
                        progress_bar.update()
                        LOGGER.error(response.text)
                        return pandas.DataFrame()

                    json_result = self._return_response(response, return_json)
                    pagination = json_result['pagination']

                    if not got_first_page:
                        total_count = pagination['total_count']
                        total_pages = pagination['total_count'] // pagination['page_size']
                        total_pages = total_pages + 1 if total_count % pagination['page_size'] > 0 else 0
                        progress_bar.reset(total=total_pages)
                        LOGGER.info('Total data size: %s, total pages to scan: %s', total_count, total_pages)
                        got_first_page = True

                    data_list.extend(json_result['data'])
                    payload.pagination_start = pagination['start'] + pagination['page_size']
                    progress_bar.update()
                except:
                    pass
        payload.pagination_start = 0
        LOGGER.info('Total response data: %s', len(data_list))
        df = pandas.DataFrame(data_list)
        return df

    def post_v1(self, url, payload: PayloadModelBase = None, return_json=False, **kwargs) -> pandas.DataFrame:
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
                try:
                    progress_bar.refresh()
                    payload.pagination_start = pagination['start'] + pagination['page_size']
                    response = self.session.post(url, data=payload.payload(), timeout=_api_timeout, **kwargs)
                    json_result = self._return_response(response, return_json)

                    pagination = json_result['pagination']
                    data_list.extend(json_result['data'])
                    progress_bar.update()
                except:
                    pass
        payload.pagination_start = 0
        LOGGER.info('Total response data: %s', len(data_list))
        df = pandas.DataFrame(data_list)
        return df


def retry_policy(info: RetryInfo) -> RetryPolicyStrategy:
    """
    - It will always retry until succeeded
    - If fails for the first time, it will retry immediately,
    - If it fails again,
      aioretry will perform a 100ms delay before the second retry,
      200ms delay before the 3rd retry,
      the 4th retry immediately,
      100ms delay before the 5th retry,
      etc...
    """
    # LOGGER.info('retry_policy: since -> %s, going to sleep sec --> %s', info.since, info.fails)
    # return False, (info.fails - 1) % 3 * 0.1

    return False, info.fails


class ApiClientAsync(ApiClientBase):

    async def _post_async(self, url, payload: PayloadModelBase, data_list, progress_bar, page_size, total_pages,
                          connector: aiohttp.TCPConnector):
        """

        :param url:
        :param payload:
        :param data_list:
        :param progress_bar:
        :param page_size:
        :param total_pages:
        :param connector:
        :return:
        """

        semaphore = asyncio.Semaphore(PARALLEL_REQUESTS)
        session = aiohttp.ClientSession(connector=connector, headers=self.headers)

        @retry(retry_policy)
        async def fetch_from_api(page_number):
            async with semaphore:
                _data = payload.payload(pagination_start=page_number * page_size)
                async with session.post(url, data=_data, timeout=_api_timeout, verify_ssl=False) as async_resp:
                    async_resp.raise_for_status()
                    json_data = await async_resp.json()
                    if 'error' in json_data:
                        # raise Exception(json_data['error'])
                        logging.error(json_data['error'])
                        return None
                    return json_data

        tasks = [
            fetch_from_api(_page) for _page in range(1, total_pages + 1)
        ]
        for task in asyncio.as_completed(tasks):
            try:
                json_result = await task
                if json_result:
                    data_list.extend(json_result['data'])
            except Exception as e:
                LOGGER.exception(e, exc_info=True)
            progress_bar.update()
        await session.close()

    def post(self, url, payload: PayloadModelBase = None, return_json=False, **kwargs) -> pandas.DataFrame:
        r"""
        Sends a POST request.

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

        with tqdm(desc=F"Querying API {url} pages", total=1, dynamic_ncols=True, miniters=0) as progress_bar:
            response = self.session.post(url, data=payload.payload(), timeout=_api_timeout, **kwargs)
            if response.status_code == 401:
                progress_bar.update()
                LOGGER.error(response.text)
                return pandas.DataFrame()

            json_result = self._return_response(response, return_json)
            pagination = json_result['pagination']
            total_count = pagination['total_count']
            total_pages = pagination['total_count'] // pagination['page_size']
            total_pages = total_pages + 1 if total_count % pagination['page_size'] > 0 else 0
            page_size = pagination['page_size']
            progress_bar.reset(total=total_pages)

            LOGGER.info('Total data size: %s, total pages to scan: %s', total_count, total_pages)

            data_list.extend(json_result['data'])

            if total_pages > 1:
                connector = aiohttp.TCPConnector(limit=PARALLEL_REQUESTS, limit_per_host=PARALLEL_REQUESTS)
                loop = asyncio.get_event_loop()
                loop.run_until_complete(
                    self._post_async(url, payload, data_list, progress_bar, page_size, total_pages, connector))
                connector.close()

        payload.pagination_start = 0
        LOGGER.info('Total response data: %s', len(data_list))
        df = pandas.DataFrame(data_list)
        return df
