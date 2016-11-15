# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
from datetime import datetime


__all__ = ['WorkflowClient']


class InvalidQueryToCountWorkflow(Exception):
    pass


class WorkflowClient(object):
    """A client that provides a pythonic API for starting and terminating workflows through an SWF boto3 client.

    :param workflow_client_config: Contains SWF values commonly used when making SWF api calls.
    :type workflow_client_config: :class:`~py_swf.config_definitions.WorkflowClientConfig`
    :param boto_client: A raw SWF boto3 client.
    :type boto_client: :class:`~SWF.Client`
    """

    def __init__(self, workflow_client_config, boto_client):
        self.workflow_client_config = workflow_client_config
        self.boto_client = boto_client

    def start_workflow(self, input, id):
        """Enqueues and starts a workflow to SWF.

        Passthrough to :meth:`~SWF.Client.start_workflow_execution`.

        :param input: Freeform string arguments describing inputs to the workflow.
        :type input: string
        :param id: Freeform string that represents a unique identifier for the workflow.
        :type id: string
        :returns: An AWS generated uuid that represents a unique identifier for the run of this workflow.
        :rtype: string
        """
        return self.boto_client.start_workflow_execution(
            domain=self.workflow_client_config.domain,
            childPolicy='TERMINATE',
            workflowId=id,
            input=input,
            workflowType={
                'name': self.workflow_client_config.workflow_name,
                'version': self.workflow_client_config.workflow_version,
            },
            taskList={
                'name': self.workflow_client_config.task_list,
            },
            executionStartToCloseTimeout=str(self.workflow_client_config.execution_start_to_close_timeout),
            taskStartToCloseTimeout=str(self.workflow_client_config.task_start_to_close_timeout),
        )['runId']

    def terminate_workflow(self, workflow_id, reason):
        """Forcefully terminates a workflow by preventing further responding and executions of
        future decision tasks and activity tasks.

        Passthrough to :meth:`~SWF.Client.terminate_workflow_execution`.

        :param workflow_id: Freeform string that represents a unique identifier for the workflow.
        :type id: string
        :param reason: Freeform string describing why the workflow was forcefully terminated.
        :type input: string
        :returns: None.
        :rtype: NoneType
        """
        self.boto_client.terminate_workflow_execution(
            domain=self.workflow_client_config.domain,
            workflowId=workflow_id,
            reason=reason,
        )

    def count_open_workflow_by_time(self, oldest_start_date, latest_start_date=None):
        """
        start_date should be type of datetime
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return:
        """
        return self._count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
        )

    def count_open_workflow_by_type(self, name, oldest_start_date=None, latest_start_date=None, version=None):
        """
        If specify a time range, oldest_start_date is required
        :param name: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :param version: string
        :return:
        """
        type_filter_dict = _build_type_filter_dict(name, version)
        return self._count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            workflow_filter_dict=type_filter_dict,
        )

    def count_open_workflow_by_tag(self, tag, oldest_start_date=None, latest_start_date=None):
        """
        If specify a time range, oldest_start_date is required
        :param tag: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return:
        """
        tag_filter_dict = _build_tag_filter_dict(tag)
        return self._count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            workflow_filter_dict=tag_filter_dict,
        )

    def count_open_workflow_by_id(self, workflow_id, oldest_start_date=None, latest_start_date=None):
        """
        If specify a time range, oldest_start_date is required
        :param workflow_id: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return:
        """
        execution_filter_dict = _build_execution_filter_dict(workflow_id)
        return self._count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            workflow_filter_dict=execution_filter_dict,
        )

    def count_closed_workflow_by_time(
            self,
            oldest_start_date=None,
            latest_start_date=None,
            oldest_close_date=None,
            latest_close_date=None
    ):
        """
        If specify a time range, either oldest_start_date or oldest_close_date is required.
        Note that oldest_start_date and oldest_close_date is mutually exclusive.
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :return:
        """
        return self._count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
        )

    def count_closed_workflow_by_type(
            self,
            name,
            version=None,
            oldest_start_date=None,
            latest_start_date=None,
            oldest_close_date=None,
            latest_close_date=None
    ):
        """
        If specify a time range, either oldest_start_date or oldest_close_date is required.
        Note that oldest_start_date and oldest_close_date are mutually exclusive
        :param name: string
        :param version: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :return:
        """
        type_filter_dict = _build_type_filter_dict(name, version)
        return self._count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
            workflow_filter_dict=type_filter_dict,
        )

    def count_closed_workflow_by_tag(
            self,
            tag,
            oldest_start_date=None,
            latest_start_date=None,
            oldest_close_date=None,
            latest_close_date=None
    ):
        """
        If specify a time range, either oldest_start_date or oldest_close_date is required.
        Note that oldest_start_date and oldest_close_date are mutually exclusive
        :param tag: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :return:
        """
        tag_filter_dict = _build_tag_filter_dict(tag)
        return self._count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
            workflow_filter_dict=tag_filter_dict,
        )

    def count_closed_workflow_by_id(
            self,
            workflow_id,
            oldest_start_date=None,
            latest_start_date=None,
            oldest_close_date=None,
            latest_close_date=None
    ):
        """
        If specify a time range, either oldest_start_date or oldest_close_date is required.
        Note that oldest_start_date and oldest_close_date are mutually exclusive
        :param workflow_id: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :return:
        """
        execution_filter_dict = _build_execution_filter_dict(workflow_id)
        return self._count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
            workflow_filter_dict=execution_filter_dict,
        )

    def count_closed_workflow_by_close_status(
            self,
            status,
            oldest_start_date=None,
            latest_start_date=None,
            oldest_close_date=None,
            latest_close_date=None
    ):
        """
        If specify a time range, either oldest_start_date or oldest_close_date is required.
        Note that oldest_start_date and oldest_close_date are mutually exclusive
        :param status: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :return:
        """
        close_status_filter_dict = _build_close_status_filter_dict(status)
        return self._count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
            workflow_filter_dict=close_status_filter_dict,
        )

    def _count_open_workflow_executions(
            self,
            oldest_start_date=None,
            latest_start_date=None,
            workflow_filter_dict=None
    ):
        """
        Count the number of open workflows for a domain. You can pass in filter criteria as a dict
        The results are best effort and may not exactly reflect recent updates and changes.
        http://docs.aws.amazon.com/amazonswf/latest/apireference/API_CountOpenWorkflowExecutions.html
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :param workflow_filter_dict: dict. filter type as key and filter criteria as value. filter criteria is a dictionary
        :return: Returns the number of open workflow executions within the given domain that meet the specified filtering criteria.
        """
        if not workflow_filter_dict:
            workflow_filter_dict = {}

        start_time_filter_dict = _build_time_filter_dict(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
        )
        if not start_time_filter_dict:
            return self.boto_client.count_open_workflow_executions(
                domain=self.workflow_client_config.domain,
                **workflow_filter_dict
            )

        return self.boto_client.count_open_workflow_executions(
            domain=self.workflow_client_config.domain,
            startTimeFilter=start_time_filter_dict['startTimeFilter'],
            **workflow_filter_dict
        )

    def _count_closed_workflow_executions(
            self,
            oldest_start_date=None,
            latest_start_date=None,
            oldest_close_date=None,
            latest_close_date=None,
            workflow_filter_dict=None
    ):
        """
        Count the number of closed workflows for a domain. You can pass in filtering criteria as a dict
        The results are best effort and may not exactly reflect recent updates and changes.
        http://docs.aws.amazon.com/amazonswf/latest/apireference/API_CountClosedWorkflowExecutions.html#SWF-CountClosedWorkflowExecutions-request-executionFilter
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :param workflow_filter_dict: dict. filter type as key and filter criteria as value. filter criteria is a dictionary
        :return: total number of closed workflows that meet the filter criteria
        """
        if not workflow_filter_dict:
            workflow_filter_dict = {}

        time_filter_dict = _build_time_filter_dict(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
        )
        if not time_filter_dict:
            return self.boto_client.count_closed_workflow_executions(
                domain=self.workflow_client_config.domain,
                **workflow_filter_dict
            )

        for key in time_filter_dict:
            time_filter_type = key
        if time_filter_type == 'startTimeFilter':
            return self.boto_client.count_closed_workflow_executions(
                domain=self.workflow_client_config.domain,
                startTimeFilter=time_filter_dict[time_filter_type],
                **workflow_filter_dict
            )
        else:
            return self.boto_client.count_closed_workflow_executions(
                domain=self.workflow_client_config.domain,
                closeTimeFilter=time_filter_dict[time_filter_type],
                **workflow_filter_dict
            )


def _build_time_filter_dict(oldest_start_date=None, latest_start_date=None, oldest_close_date=None, latest_close_date=None):
    """
    Build time_filter_dict for count open/closed workflow executions.
    Return result is a dict: {time_filter_type : filter_dict}
    filter_dict must contains key 'oldestDate'.
    return example:
    {
        'startTimeFilter': {
            'oldestDate': datetime(2016, 11, 11)
        }
    }
    If no oldest_date specified, return None
    """
    if oldest_start_date and oldest_close_date:
        raise InvalidQueryToCountWorkflow("Must specify only oldest_start_date or oldest_close_date!")
    if oldest_start_date:
        filter_type = 'startTimeFilter'
        _validate_date_type(oldest_start_date)
        filter_dict = {'oldestDate': oldest_start_date}
        if latest_start_date:
            _validate_date_type(latest_start_date)
            filter_dict['latestDate'] = latest_start_date
    elif oldest_close_date:
        filter_type = 'closeTimeFilter'
        _validate_date_type(oldest_close_date)
        filter_dict = {'oldestDate': oldest_close_date}
        if latest_close_date:
            _validate_date_type(latest_close_date)
            filter_dict['latestDate'] = latest_close_date
    else:
        return None
    return {filter_type: filter_dict}


def _validate_date_type(date):
    if not isinstance(date, datetime):
        raise InvalidQueryToCountWorkflow("time filter value must be datetime type")


def _build_type_filter_dict(name, version):
    filter_dict = {'name': name}
    if version:
        filter_dict['version'] = version
    return {'typeFilter': filter_dict}


def _build_tag_filter_dict(tag):
    filter_dict = {'tag': tag}
    return {'tagFilter': filter_dict}


def _build_execution_filter_dict(workflow_id):
    filter_dict = {'workflowId': workflow_id}
    return {'executionFilter': filter_dict}


def _build_close_status_filter_dict(status):
    valid_status = set(['COMPLETED', 'FAILED', 'CANCELED', 'TERMINATED', 'CONTINUED_AS_NEW', 'TIMED_OUT'])
    if status not in valid_status:
        raise InvalidQueryToCountWorkflow("Invalid workflow close status!")
    filter_dict = {'status': status}
    return {'closeStatusFilter': filter_dict}
