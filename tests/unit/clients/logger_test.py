# -*- coding: utf-8 -*-
import mock

from py_swf.clients.logger import amazon_api_list
from py_swf.clients.logger import build_boto_client_with_api_call_logger
from py_swf.clients.logger import _get_api_name
from py_swf.clients.logger import _get_boto_method
from py_swf.clients.logger import boto_amazon_api_calls
from py_swf.clients.logger import boto_available_methods
from py_swf.clients.logger import boto_non_amazon_api_calls


def test_get_api_name():
    for name in boto_amazon_api_calls:
        assert _get_api_name(name) in amazon_api_list
    for name in boto_non_amazon_api_calls:
        assert _get_api_name(name) not in amazon_api_list


def test_get_boto_name():
    for api in amazon_api_list:
        assert _get_boto_method(api) in boto_amazon_api_calls


def _build_boto_client_with_methods(boto_client):
    for method in boto_available_methods:
        setattr(boto_client, method, mock.Mock())
    return boto_client


def test_build_boto_client_with_api_call_logger(boto_client):
    boto_client = _build_boto_client_with_methods(boto_client)
    boto_client_with_logger = build_boto_client_with_api_call_logger(boto_client)
    for method_name in boto_available_methods:
        if method_name in boto_amazon_api_calls:
            decorated_method = getattr(boto_client_with_logger, method_name)
            assert getattr(decorated_method, '__wrapped__') == method_name