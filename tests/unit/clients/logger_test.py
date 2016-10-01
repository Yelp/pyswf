# -*- coding: utf-8 -*-
import mock

from py_swf.clients.logger import amazon_api_list
from py_swf.clients.logger import build_boto_client_with_api_call_logger
from py_swf.clients.logger import _get_api_name

boto_available_methods = [
    "can_paginate",
    "count_closed_workflow_executions",
    "count_open_workflow_executions",
    "count_pending_activity_tasks",
    "count_pending_decision_tasks",
    "deprecate_activity_type",
    "deprecate_domain",
    "deprecate_workflow_type",
    "describe_activity_type",
    "describe_domain",
    "describe_workflow_execution",
    "describe_workflow_type",
    "generate_presigned_url",
    "get_paginator",
    "get_waiter",
    "get_workflow_execution_history",
    "list_activity_types",
    "list_closed_workflow_executions",
    "list_domains",
    "list_open_workflow_executions",
    "list_workflow_types",
    "poll_for_activity_task",
    "poll_for_decision_task",
    "record_activity_task_heartbeat",
    "register_activity_type",
    "register_domain",
    "register_workflow_type",
    "request_cancel_workflow_execution",
    "respond_activity_task_canceled",
    "respond_activity_task_completed",
    "respond_activity_task_failed",
    "respond_decision_task_completed",
    "signal_workflow_execution",
    "start_workflow_execution",
    "terminate_workflow_execution",
]

boto_non_amazon_api_calls = [
    "can_paginate",
    "generate_presigned_url",
    "get_paginator",
    "get_waiter",
]

boto_amazon_api_calls = [method for method in boto_available_methods if method not in boto_non_amazon_api_calls]


def test_get_api_name():
    for name in boto_amazon_api_calls:
        assert _get_api_name(name) in amazon_api_list
    for name in boto_non_amazon_api_calls:
        assert _get_api_name(name) not in amazon_api_list


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
