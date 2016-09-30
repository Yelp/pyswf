# -*- coding: utf-8 -*-
import logging
import inspect
import types

DEFAULT_LOGGER_NAME = 'default_logger'

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


def build_boto_client_with_api_call_logger(boto_client, logger=None):

    class BotoClientWithLogger(object):
        def __init__(self, boto_client, logger=None):
            self.boto_client = boto_client
            self.logger = logger or _build_default_logger()
            self._set_logger_on_boto_client()

        def _set_logger_on_boto_client(self):
            for field, func in inspect.getmembers(self.boto_client):
                if isinstance(func, types.UnboundMethodType) and _get_api_name(func) in amazon_api_list:
                    setattr(self, field, _add_logger_to_api_call(self.boto_client, field, self.logger))
                else:
                    setattr(self, field, func)

    return BotoClientWithLogger(boto_client, logger)


def _add_logger_to_api_call(boto_client, func_name, logger):
    def _log_amazon_api_call(*args, **kwargs):
        logger(_get_api_name(func_name))
        return getattr(boto_client, func_name)(*args, **kwargs)
    return _log_amazon_api_call


def _build_default_logger():
    logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    console_handler = logging.StreamHandler()
    logger.addHandler(console_handler)
    return logger


def _get_api_name(name):
    translated_name = ''
    for seg in name.split('_'):
        translated_name += seg.capitalize()
    return translated_name
