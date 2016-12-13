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

    def count_open_workflow_executions(
            self,
            oldest_start_date,
            latest_start_date=None,
            workflow_name=None,
            version=None,
            tag=None,
            workflow_id=None,
    ):
        """
        Count the number of open workflows for a domain. You can pass in filtering criteria.
        This operation is eventually consistent. The results are best effort and may not exactly reflect recent updates
        and changes. Worklfows started or closed near the time when calling count_open_workflow_executions may not be reflected

        Passthrough to :meth:~SWF.Client.count_open_workflow_executions``

        executionFilter , typeFilter and tagFilter are mutually exclusive. You can specify at most one of these in a request.

        :param oldest_start_date: datetime. Specifies the oldest start date and time to return.
        :param latest_start_date: datetime. Specifies the latest start date and time to return.
        :param workflow_name: string. Required for typeFilter. Specifies the type of the workflow executions to be counted.
        :param version: string. Optional for typeFilter. Version of the workflow type.
        :param tag: string. Required for tagFilter. Specifies the tag that must be associated with the execution for it
            to meet the filter criteria.
        :param workflow_id: string. Required for executionFilter. The workflowId to pass of match the criteria of this filter.
        :return: number of open workflows within time range
        """
        workflow_filter_dict = _build_workflow_filter_dict(
            workflow_name=workflow_name,
            version=version,
            tag=tag,
            workflow_id=workflow_id,
        )

        start_time_filter_dict = _build_time_filter_dict(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
        )

        return self.boto_client.count_open_workflow_executions(
            domain=self.workflow_client_config.domain,
            startTimeFilter=start_time_filter_dict['startTimeFilter'],
            **workflow_filter_dict
        )

    def count_closed_workflow_executions(
            self,
            oldest_start_date=None,
            latest_start_date=None,
            oldest_close_date=None,
            latest_close_date=None,
            workflow_name=None,
            version=None,
            tag=None,
            workflow_id=None,
            close_status=None,
    ):
        """
        Count the number of closed workflows for a domain. You can pass in filtering criteria.
        This operation is eventually consistent. The results are best effort and may not exactly reflect recent updates
        and changes. Worklfows started or closed near the time when calling count_open_workflow_executions may not be reflected

        Passthrough to :meth:~SWF.Client.count_closed_workflow_executions``

        startTimeFilter and closeTimeFilter are mutually exclusive. You MUST specify one of these in a request but not both.
        closeStatusFilter , executionFilter , typeFilter and tagFilter are mutually exclusive. You can specify at most
            one of these in a request.

        :param oldest_start_date: datetime. Specifies the oldest start date and time to return.
        :param latest_start_date: datetime. Specifies the latest start date and time to return.
        :param oldest_close_date: datetime. Specifies the oldest close date and time to return.
        :param latest_close_date: datetime. Specifies the latest close date and time to return.
        :param workflow_name: string. Required for typeFilter. Specifies the type of the workflow executions to be counted.
        :param version: string. Optional for typeFilter. Version of the workflow type.
        :param tag: string. Required for tagFilter. Specifies the tag that must be associated with the execution for it
            to meet the filter criteria.
        :param workflow_id: string. Required for executionFilter. The workflowId to pass of match the criteria of this filter.
        :param close_status: string.
            valid status are ('COMPLETED', 'FAILED', 'CANCELED', 'TERMINATED', 'CONTINUED_AS_NEW', 'TIMED_OUT')
            This filter has an affect only if executionStatus is specified as CLOSED
        :return: total number of closed workflows that meet the filter criteria
        """
        time_filter_dict = _build_time_filter_dict(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
            oldest_close_date=oldest_close_date,
            latest_close_date=latest_close_date,
        )

        workflow_filter_dict = _build_workflow_filter_dict(
            workflow_name=workflow_name,
            version=version,
            tag=tag,
            workflow_id=workflow_id,
            close_status=close_status,
        )
        workflow_filter_dict.update(time_filter_dict)

        return self.boto_client.count_closed_workflow_executions(
            domain=self.workflow_client_config.domain,
            **workflow_filter_dict
        )


def _build_time_filter_dict(oldest_start_date=None, latest_start_date=None, oldest_close_date=None, latest_close_date=None):
    """
    Build time_filter_dict for calls to _count_closed_workflow_executions and _count_open_workflow_executions.
    Return result is a dict: {time_filter_type : filter_dict}
    filter_dict must contains key 'oldestDate'.
    sample input:
        oldest_start_date=datetime(2016, 11, 11)
        return example:
        {
            'startTimeFilter': {
                'oldestDate': datetime(2016, 11, 11)
            }
        }
    If no oldest_date specified, return empty dict
    """
    result = {}

    if oldest_start_date:
        result.update({'startTimeFilter': _build_time_range(oldest_start_date, latest_start_date)})

    if oldest_close_date:
        result.update({'closeTimeFilter': _build_time_range(oldest_close_date, latest_close_date)})

    return result


def _build_time_range(oldest_date, latest_date):
    result = {'oldestDate': oldest_date}
    if latest_date is not None:
        result['latestDate'] = latest_date
    return result


def _build_type_filter_dict(name, version):
    filter_dict = {'name': name}
    if version is not None:
        filter_dict['version'] = version
    return {'typeFilter': filter_dict}


def _build_tag_filter_dict(tag):
    return {'tagFilter': {'tag': tag}}


def _build_execution_filter_dict(workflow_id):
    return {'executionFilter': {'workflowId': workflow_id}}


def _build_close_status_filter_dict(status):
    return {'closeStatusFilter': {'status': status}}


def _build_workflow_filter_dict(
        workflow_name=None,
        version=None,
        tag=None,
        workflow_id=None,
        close_status=None,
):
    workflow_filter_dict = {}
    if workflow_name:
        workflow_filter_dict.update(_build_type_filter_dict(workflow_name, version))
    if tag:
        workflow_filter_dict.update(_build_tag_filter_dict(tag))
    if workflow_id:
        workflow_filter_dict.update(_build_execution_filter_dict(workflow_id))
    if close_status:
        workflow_filter_dict.update(_build_close_status_filter_dict(close_status))
    return workflow_filter_dict
