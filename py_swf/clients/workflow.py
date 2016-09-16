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
