# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from botocore.vendored.requests.exceptions import ReadTimeout

from py_swf.errors import NoTaskFound


__all__ = ['ActivityTaskClient', 'ActivityTask']


ActivityTask = namedtuple('ActivityTask', 'activity_id type version input task_token workflow_id workflow_run_id')
"""Contains the metadata to execute an activity task.

See the response syntax in :meth:`~SWF.Client.poll_for_activity_task`.
"""


class ActivityTaskClient(object):
    """A client that provides a pythonic API for polling and responding to activity tasks through an SWF boto3 client.

    :param activity_task_config: Contains SWF values commonly used when making SWF api calls.
    :type activity_task_config: :class:`~py_swf.config_definitions.ActivityTaskConfig`
    :param boto_client: A raw SWF boto3 client.
    :type boto_client: :class:`~SWF.Client`
    """

    def __init__(self, activity_task_config, boto_client):
        self.activity_task_config = activity_task_config
        self.boto_client = boto_client

    def poll(self, identity=None):
        """Opens a connection to AWS and long-polls for activity tasks.
        When an activity is available, this function will return with exactly one activity task to execute.

        Passthrough to :meth:`~SWF.Client.poll_for_activity_task`.

        :param identity: A freeform text that identifies the client that performed the longpoll. Useful for debugging history.
        :type identity: string
        :return: An activity task to execute.
        :rtype: ActivityTask
        :raises py_swf.errors.NoTaskFound: Raised when polling for an activity times out without receiving any tasks.
        """
        kwargs = dict(
            domain=self.activity_task_config.domain,
            taskList={
                'name': self.activity_task_config.task_list,
            },
        )

        if identity is not None:
            kwargs['identity'] = identity

        try:
            results = self.boto_client.poll_for_activity_task(
                **kwargs
            )
        except ReadTimeout as e:
            raise NoTaskFound(e)

        # Sometimes SWF gives us an incomplete response, ignore these.
        if not results.get('taskToken', None):
            raise NoTaskFound('Received results with no taskToken')

        return ActivityTask(
            activity_id=results['activityId'],
            type=results['activityType']['name'],
            version=results['activityType']['version'],
            input=results['input'],
            task_token=results['taskToken'],
            workflow_id=results['workflowExecution']['workflowId'],
            workflow_run_id=results['workflowExecution']['runId'],
        )

    def finish(self, task_token, result):
        """Responds to an activity task with a success.

        Passthrough to :meth:`~SWF.Client.respond_activity_task_completed`.

        :param task_token: The task_token returned from :meth:`~py_swf.clients.activity_task.ActivityTaskClient.poll`.
        :type task_token: string
        :param result: The result of the executed activity task.
        :type result: string
        :return: None
        :rtype: NoneType
        """
        self.boto_client.respond_activity_task_completed(
            result=result,
            taskToken=task_token,
        )

    def fail(self, task_token, reason, details=None):
        """Responds to an activity task with a failure.

        Passthrough to :meth:`~SWF.Client.respond_activity_task_failed`.

        :param task_token: The task_token returned from :meth:`~py_swf.clients.activity_task.ActivityTaskClient.poll`.
        :type task_token: string
        :param reason: Description of the error that may assist in diagnostics
        :type reason: string
        :param details: Optional. Detailed information about the failure
        :type details: string
        :return: None
        :rtype: NoneType
        """
        kwargs = dict(
            reason=reason,
        )
        if details is not None:
            kwargs["details"] = details

        self.boto_client.respond_activity_task_failed(
            taskToken=task_token,
            **kwargs
        )
