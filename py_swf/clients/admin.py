# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import functools

from botocore.exceptions import ClientError


__all__ = ['WorkflowRegistrar']


def idempotent_create(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] not in set([
                'DomainAlreadyExistsFault',
                'TypeAlreadyExistsFault',
            ]):
                raise
    return wrapped


class WorkflowRegistrar(object):
    """A client that allows creation of SWF environments.
    Allows you to create domains, task lists, workflow types, and activity types.

    :param boto_client: A raw SWF boto3 client.
    :type boto_client: :class:`~SWF.Client`
    """

    def __init__(self, boto_client):
        self.boto_client = boto_client

    @idempotent_create
    def register_domain(self, name, description=None, retention=90):
        """Creates an swf domain.

        Passthrough to :meth:`~SWF.Client.register_domain`.

        :param name: Name of the domain
        :type name: string
        :param description: An optional description of the domain.
        :type description: string
        :param retention: An optional retention of the domain. Measured in days. If None is passed in, the retention is 0.
                          Defaults to 90 days.
        :type retention: int
        :return: None
        :rtype: NoneType
        """
        if retention is None:
            retention = 'NONE'

        kwargs = dict(
            name=name,
            workflowExecutionRetentionPeriodInDays=str(retention),
        )

        if description is not None:
            kwargs['description'] = description

        self.boto_client.register_domain(
            **kwargs
        )

    @idempotent_create
    def register_activity_type(
        self,
        domain,
        name,
        version,
        task_heartbeat_timeout=None,
        task_list_name=None,
        task_priority=None,
        task_schedule_to_close_timeout=None,
        task_schedule_to_start_timeout=None,
        task_start_to_close_timeout=None,
        description=None,
    ):
        """Creates an activity type

        Passthrough to :meth:`~SWF.Client.register_activity_type`.

        :param domain: Registers the activity type to this domain.
        :type domain: string
        :param name: Name of the activity type to register.
        :type name: string
        :param version: Freeform text that represents the version of the activity type.
        :type version: string
        :param task_heartbeat_timeout: Represents a timeout for subsequent heartbeats sent by an activity task.
                                       Measured in seconds.
        :type task_heartbeat_timeout: double
        :param task_list_name: Registers the activity type to this task list name.
        :type task_list_name: string
        :param task_priority: Represents a priority for this task to run.
                              Valid within the range of (-2147483648, 2147483647). Higher numbers indicate higher priority.
        :type task_priority: int
        :param task_schedule_to_close_timeout: The timeout for activities when scheduled until response. Measured in seconds.
        :type task_schedule_to_close_timeout: double
        :param task_schedule_to_start_timeout: The timeout for activities when submitted until they are scheduled.
                                               Measured in seconds.
        :type task_schedule_to_start_timeout: double
        :param task_start_to_close_timeout: End-to-end timeout for when activities are submitted until response.
                                            Measured in seconds.
        :type task_start_to_close_timeout: double
        :param description: Describes what the activity task does
        :type description: string
        :return: None
        :rtype: NoneType
        """
        kwargs = dict(
            domain=domain,
            name=name,
            version=version,
        )

        if task_heartbeat_timeout is not None:
            kwargs['defaultTaskHeartbeatTimeout'] = str(task_heartbeat_timeout)
        if task_list_name is not None:
            kwargs['defaultTaskList'] = {
                "name": task_list_name,
            }
        if task_priority is not None:
            kwargs['defaultTaskPriority'] = str(task_priority)
        if task_schedule_to_close_timeout is not None:
            kwargs['defaultTaskScheduleToCloseTimeout'] = str(task_schedule_to_close_timeout)
        if task_schedule_to_start_timeout is not None:
            kwargs['defaultTaskScheduleToStartTimeout'] = str(task_schedule_to_start_timeout)
        if task_start_to_close_timeout is not None:
            kwargs['defaultTaskStartToCloseTimeout'] = str(task_start_to_close_timeout)
        if description is not None:
            kwargs['description'] = description

        self.boto_client.register_activity_type(
            **kwargs
        )

    @idempotent_create
    def register_workflow_type(
        self,
        domain,
        name,
        version,
        child_policy=None,
        execution_start_to_close_timeout=None,
        lambda_role=None,
        task_list_name=None,
        task_priority=None,
        task_start_to_close_timeout=None,
        description=None,
    ):
        """Creates a workflow type

        Passthrough to :meth:`~SWF.Client.register_workflow_type`.

        :param domain: Registers the workflow type to this domain.
        :type domain: string
        :param name: Name of the workflow type to register.
        :type name: string
        :param version: Freeform text that represents the version of the workflow type.
        :type version: string
        :param child_policy: Determins the child workflow policy upon termination.
        :type child_policy: string of ('TERMINATE'|'REQUEST_CANCEL'|'ABANDON')
        :param execution_start_to_close_timeout: Represents the end-to-end timeout for a workflow. Measured in seconds.
        :type execution_start_to_close_timeout: double
        :param lambda_role: The IAM role attached to this workflow execution to use when invoking AWS Lambda functions.
        :type lambda_role: string
        :param task_list_name: Registers this workflow type to this task list.
        :type task_list_name: string
        :param task_priority: Sets the task priority for this workflow type.
                              Valid values are in the range of (-2147483648, 2147483647).
                              Higher numbers indicate higher priority.
        :type task_priority: int
        :param task_start_to_close_timeout: Represents end-to-end activity task timeouts. Measured in seconds.
        :type task_start_to_close_timeout: double
        :param description: Describes what the workflow type does.
        :type description: string
        :return: None
        :rtype: NoneType
        """
        kwargs = dict(
            domain=domain,
            name=name,
            version=version,
        )

        if child_policy is not None:
            kwargs["defaultChildPolicy"] = child_policy
        if execution_start_to_close_timeout is not None:
            kwargs["defaultExecutionStartToCloseTimeout"] = execution_start_to_close_timeout
        if lambda_role is not None:
            kwargs["defaultLambdaRole"] = lambda_role
        if task_list_name is not None:
            kwargs["defaultTaskList"] = {
                "name": task_list_name,
            }
        if task_priority is not None:
            kwargs["defaultTaskPriority"] = task_priority
        if task_start_to_close_timeout is not None:
            kwargs["defaultTaskStartToCloseTimeout"] = task_start_to_close_timeout
        if description is not None:
            kwargs["description"] = description

        self.boto_client.register_workflow_type(
            **kwargs
        )
