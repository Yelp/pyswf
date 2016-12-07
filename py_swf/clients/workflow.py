# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


__all__ = ['WorkflowClient']


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

    def count_open_workflow_by_start_time(self, oldest_start_date, latest_start_date=None):
        """
        only workflow executions that meet start time criteria will be counted.
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return: number of open workflows within time range
        """
        return self._count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
        )

    def count_open_workflow_by_type_and_start_time(self, name, oldest_start_date, latest_start_date=None, version=None):
        """
        only workflow have the desired type and start time criteria will be counted.
        :param name: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :param version: string
        :return: total number of open workflows that met the filter criteria
        """
        type_filter_dict = _build_type_filter_dict(name, version)
        return self._count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            workflow_filter_dict=type_filter_dict,
        )

    def count_open_workflow_by_tag_and_start_time(self, tag, oldest_start_date, latest_start_date=None):
        """
        only executions that have a tag that matches the filter and start time criteria are counted.
        :param tag: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return: total number of open workflows that met the filter criteria
        """
        tag_filter_dict = _build_tag_filter_dict(tag)
        return self._count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            workflow_filter_dict=tag_filter_dict,
        )

    def count_open_workflow_by_id_and_start_time(self, workflow_id, oldest_start_date, latest_start_date=None):
        """
        only workflow executions matching the WorkflowId in the filter and start time criteria are counted.
        :param workflow_id: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return: total number of open workflows that met the filter criteria
        """
        execution_filter_dict = _build_execution_filter_dict(workflow_id)
        return self._count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            workflow_filter_dict=execution_filter_dict,
        )

    def count_closed_workflow_by_start_time(self, oldest_start_date, latest_start_date=None):
        """
        only workflow executions that meet the start time criteria of the filter are counted.
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return: total number of closed workflows that met the filter criteria
        """
        return self._count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
        )

    def count_closed_workflow_by_close_time(self, oldest_close_date, latest_close_date=None):
        """
        only workflow executions that meet the close time criteria of the filter are counted.
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :return: total number of closed workflows that met the filter criteria
        """
        return self._count_closed_workflow_executions(
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
        )

    def count_closed_workflow_by_type_and_start_time(self, name, oldest_start_date, latest_start_date=None, version=None):
        """
        only workflow executions that meet the type criteria and start time criteria are counted
        :param name: string
        :param version: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return: total number of closed workflows that met the filter criteria
        """
        type_filter_dict = _build_type_filter_dict(name, version)
        return self._count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            workflow_filter_dict=type_filter_dict,
        )

    def count_closed_workflow_by_type_and_close_time(self, name, oldest_close_date, latest_close_date=None, version=None):
        """
        only workflow executions that meet the type criteria and close time criteria are counted
        :param name: string
        :param version: string
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :return: total number of closed workflows that met the filter criteria
        """
        type_filter_dict = _build_type_filter_dict(name, version)
        return self._count_closed_workflow_executions(
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
            workflow_filter_dict=type_filter_dict,
        )

    def count_closed_workflow_by_tag_and_start_time(self, tag, oldest_start_date, latest_start_date=None):
        """
        only executions that have a tag that matches the filter and meet start time criteria are counted.
        :param tag: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return: total number of closed workflows that met the filter criteria
        """
        tag_filter_dict = _build_tag_filter_dict(tag)
        return self._count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            workflow_filter_dict=tag_filter_dict,
        )

    def count_closed_workflow_by_tag_and_close_time(self, tag, oldest_close_date, latest_close_date=None):
        """
        only executions that have a tag that matches the filter and meet close time criteria are counted.
        :param tag: string
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :return: total number of closed workflows that met the filter criteria
        """
        tag_filter_dict = _build_tag_filter_dict(tag)
        return self._count_closed_workflow_executions(
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
            workflow_filter_dict=tag_filter_dict,
        )

    def count_closed_workflow_by_id_and_start_time(self, workflow_id, oldest_start_date, latest_start_date=None):
        """
        only workflow executions matching the WorkflowId in the filter and meet start time criteria are counted.
        :param workflow_id: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return: total number of closed workflows that met the filter criteria
        """
        execution_filter_dict = _build_execution_filter_dict(workflow_id)
        return self._count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            workflow_filter_dict=execution_filter_dict,
        )

    def count_closed_workflow_by_id_and_close_time(self, workflow_id, oldest_close_date, latest_close_date=None):
        """
        only workflow executions matching the WorkflowId in the filter and meet close time criteria are counted.
        :param workflow_id: string
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :return: total number of closed workflows that met the filter criteria
        """
        execution_filter_dict = _build_execution_filter_dict(workflow_id)
        return self._count_closed_workflow_executions(
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
            workflow_filter_dict=execution_filter_dict,
        )

    def count_closed_workflow_by_close_status_and_start_time(self, status, oldest_start_date, latest_start_date=None):
        """
        only workflow executions that match this close status and start time criteria are counted.
        valid status are ('COMPLETED', 'FAILED', 'CANCELED', 'TERMINATED', 'CONTINUED_AS_NEW', 'TIMED_OUT')
        This filter has an affect only if executionStatus is specified as CLOSED
        :param status: string
        :param oldest_start_date: datetime
        :param latest_start_date: datetime
        :return: total number of closed workflows that met the filter criteria
        """
        close_status_filter_dict = _build_close_status_filter_dict(status)
        return self._count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            workflow_filter_dict=close_status_filter_dict,
        )

    def count_closed_workflow_by_close_status_and_close_time(self, status, oldest_close_date, latest_close_date=None):
        """
        only workflow executions that match this close status and close time criteria are counted.
        valid status are ('COMPLETED', 'FAILED', 'CANCELED', 'TERMINATED', 'CONTINUED_AS_NEW', 'TIMED_OUT')
        This filter has an affect only if executionStatus is specified as CLOSED
        :param status: string
        :param oldest_close_date: datetime
        :param latest_close_date: datetime
        :return: total number of closed workflows that met the filter criteria
        """
        close_status_filter_dict = _build_close_status_filter_dict(status)
        return self._count_closed_workflow_executions(
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
    Build time_filter_dict for calls to _count_closed_workflow_executions and _count_open_workflow_executions.
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
    date_tuple = ()
    if oldest_start_date:
        filter_type = 'startTimeFilter'
        date_tuple = (oldest_start_date, latest_start_date)
    elif oldest_close_date:
        filter_type = 'closeTimeFilter'
        date_tuple = (oldest_close_date, latest_close_date)

    if not date_tuple:
        return None

    oldest_date, latest_date = date_tuple
    filter_dict = dict(oldestDate=oldest_date)
    if latest_date:
        filter_dict['latestDate'] = latest_date
    return {filter_type: filter_dict}


def _build_type_filter_dict(name, version):
    filter_dict = {'name': name}
    if version:
        filter_dict['version'] = version
    return {'typeFilter': filter_dict}


def _build_tag_filter_dict(tag):
    return {'tagFilter': {'tag': tag}}


def _build_execution_filter_dict(workflow_id):
    return {'executionFilter': {'workflowId': workflow_id}}


def _build_close_status_filter_dict(status):
    return {'closeStatusFilter': {'status': status}}
