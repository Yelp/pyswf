# -*- coding: utf-8 -*-
import logging
import inspect
import types

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
            for field, func in inspect.getmembers(self.boto_client):
                if _get_api_name(field) in amazon_api_list:
                    setattr(self, field, _add_logger_to_api_call(self.boto_client, field, self.logger))
                else:
                    setattr(self, field, func)

    return BotoClientWithLogger(boto_client, logger)


def _add_logger_to_api_call(boto_client, func_name, logger):
    """
    a decorator to log calls to amazon apis
    :param boto_client: a reference to boto_client
    :param func_name: name of method that calls amazon apis
    :param logger: logger to log info
    :return: a callable object
    """
    def log_amazon_api_call(*args, **kwargs):
        logger(_get_api_name(func_name))
        return getattr(boto_client, func_name)(*args, **kwargs)
    # set __wrapped__ for test to make sure it is actually decorated
    log_amazon_api_call.__wrapped__ = func_name
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
