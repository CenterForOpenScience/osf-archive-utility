import requests
from ratelimit import sleep_and_retry
from ratelimit.exception import RateLimitException


@sleep_and_retry
def get_with_retry(url, retry_on=list(), sleep_period=None, headers=None) -> requests.Response:

    resp = requests.get(url, headers=headers)
    if resp.status_code in retry_on:
        raise RateLimitException(
            message='Too many requests, sleeping.',
            period_remaining=sleep_period or int(resp.headers.get('Retry-After') or 0)
        )  # This will be caught by @sleep_and_retry and retried

    return resp


@sleep_and_retry
def put_with_retry(
        url,
        data,
        headers=dict(),
        retry_on=list(),
        sleep_period=None) -> requests.Response:

    resp = requests.put(url, headers=headers, data=data)
    if resp.status_code in retry_on:
        raise RateLimitException(
            message='Too many requests, sleeping.',
            period_remaining=sleep_period or int(resp.headers.get('Retry-After') or 0)
        )  # This will be caught by @sleep_and_retry and retried

    return resp
