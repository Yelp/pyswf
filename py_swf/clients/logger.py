# -*- coding: utf-8 -*-
import logging
import inspect

DEFAULT_LOGGER_NAME = 'default_logger_for_amazon_api_calls'

amazon_api_list = [
    "CountClosedWorkflowExecutions",
    "CountOpenWorkflowExecutions",
    "CountPendingActivityTasks",
    "CountPendingDecisionTasks",
    "DeprecateActivityType",
    "DeprecateDomain",
    "DeprecateWorkflowType",
    "DescribeActivityType",
    "DescribeDomain",
    "DescribeWorkflowExecution",
    "DescribeWorkflowType",
    "GetWorkflowExecutionHistory",
    "ListActivityTypes",
    "ListClosedWorkflowExecutions",
    "ListDomains",
    "ListOpenWorkflowExecutions",
    "ListWorkflowTypes",
    "PollForActivityTask",
    "PollForDecisionTask",
    "RecordActivityTaskHeartbeat",
    "RegisterActivityType",
    "RegisterDomain",
    "RegisterWorkflowType",
    "RequestCancelWorkflowExecution",
    "RespondActivityTaskCanceled",
    "RespondActivityTaskCompleted",
    "RespondActivityTaskFailed",
    "RespondDecisionTaskCompleted",
    "SignalWorkflowExecution",
    "StartWorkflowExecution",
    "TerminateWorkflowExecution",
]

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


def _get_boto_method(api_name):
    method_name = ''
    seg = ''
    index = 0
    while index < len(api_name):
        if api_name[index].isupper():
            if method_name:
                method_name += '_'
            method_name += seg
            seg = api_name[index].lower()
        else:
            seg += api_name[index]
        index += 1
    if seg:
        method_name += '_' + seg
    return method_name


def build_boto_amazon_api_call_list():
    boto_amazon_api_calls = []
    for api in amazon_api_list:
        boto_amazon_api_calls.append(_get_boto_method(api))
    return boto_amazon_api_calls

boto_amazon_api_calls = build_boto_amazon_api_call_list()
boto_non_amazon_api_calls = [method for method in boto_available_methods if method not in boto_amazon_api_calls]


def build_boto_client_with_api_call_logger(boto_client, logger=None):
    """
    build a boto_client wrapper class that will log every call to amazon apis. It will build a BotoClientWithLogger instance.
    This instance share the same interface with boto_client, but will log calls to amazon apis using a decorator
    :param boto_client: a reference to boto_client
    :param logger: must be callable, assume user has already added timestamp to logger logic
    :return: a BotoClientWithLogger instance.
    """
    class BotoClientWithLogger(object):
        def __init__(self, boto_client, logger=None):
            self.boto_client = boto_client
            self.logger = logger or _build_default_logger()
            self._set_logger_on_boto_client()

        def _set_logger_on_boto_client(self):
            # make BotoClientWithLogger shares the same interface as boto_client
            # set logger on methods that calls amazon apis
            for method in boto_available_methods:
                boto_func = getattr(self.boto_client, method)
                if method in boto_amazon_api_calls:
                    setattr(self, method, _add_logger_to_api_call(boto_func, self.logger))
                else:
                    setattr(self, method, boto_func)

    return BotoClientWithLogger(boto_client, logger)


def _add_logger_to_api_call(boto_func, logger):
    """
    a decorator to log calls to amazon apis
    :param boto_client: a reference to boto_client
    :param func_name: name of method that calls amazon apis
    :param logger: logger to log info, must be callable
    :return: a callable object
    """
    def log_amazon_api_call(*args, **kwargs):
        logger(amazon_api_name=_get_api_name(boto_func.__name__))
        return boto_func(*args, **kwargs)
    # set __wrapped__ for test to make sure it is actually decorated
    log_amazon_api_call.__wrapped__ = boto_func.__name__
    return log_amazon_api_call


def _build_default_logger():
    """
    build a default logger with StreamHandler for logging with DEFAULT_LOGGER_NAME
    :return: a logger
    """
    logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger.info


def _get_api_name(name):
    """
    translate boto_client method name into corresponding amazon api name
    :param name: string, boto_client method name
    :return: string, corresponding amazon api name
    """
    translated_name = ''
    for seg in name.split('_'):
        translated_name += seg.capitalize()
    return translated_name



