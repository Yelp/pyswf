# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from botocore.exceptions import ClientError

from py_swf.clients.admin import idempotent_create
from py_swf.clients.admin import WorkflowRegistrar


@pytest.fixture
def workflow_registrar(boto_client):
    return WorkflowRegistrar(boto_client)


@pytest.mark.parametrize('error_code', ['DomainAlreadyExistsFault', 'TypeAlreadyExistsFault'])
def test_idempotent_create_whitelist(error_code):
    err = ClientError(error_response={'Error': {'Code': error_code}}, operation_name='meowing')

    @idempotent_create
    def boba():
        raise err
    boba()


def test_idempotent_create_unexpected_error():
    @idempotent_create
    def boba():
        raise ValueError
    with pytest.raises(ValueError):
        boba()


def test_register_domain(workflow_registrar, boto_client):
    workflow_registrar.register_domain(
        name='name',
        description='description',
        retention=50,
    )

    boto_client.register_domain.assert_called_once_with(
        name='name',
        description='description',
        workflowExecutionRetentionPeriodInDays='50',
    )


def test_register_domain_default_args(workflow_registrar, boto_client):
    workflow_registrar.register_domain(
        name='name',
    )

    boto_client.register_domain.assert_called_once_with(
        name='name',
        workflowExecutionRetentionPeriodInDays='90',
    )


def test_register_domain_no_retention(workflow_registrar, boto_client):
    workflow_registrar.register_domain(
        name='name',
        retention=None,
    )

    boto_client.register_domain.assert_called_once_with(
        name='name',
        workflowExecutionRetentionPeriodInDays='NONE',
    )


def test_register_activity_type(workflow_registrar, boto_client):
    workflow_registrar.register_activity_type(
        domain='domain',
        name='name',
        version='version',
        task_heartbeat_timeout='task_heartbeat_timeout',
        task_list_name='task_list_name',
        task_priority='task_priority',
        task_schedule_to_close_timeout='task_schedule_to_close_timeout',
        task_schedule_to_start_timeout='task_schedule_to_start_timeout',
        task_start_to_close_timeout='task_start_to_close_timeout',
        description='description',
    )

    boto_client.register_activity_type.assert_called_once_with(
        defaultTaskHeartbeatTimeout='task_heartbeat_timeout',
        defaultTaskList={
            'name': 'task_list_name',
        },
        defaultTaskPriority='task_priority',
        defaultTaskScheduleToCloseTimeout='task_schedule_to_close_timeout',
        defaultTaskScheduleToStartTimeout='task_schedule_to_start_timeout',
        defaultTaskStartToCloseTimeout='task_start_to_close_timeout',
        description='description',
        domain='domain',
        name='name',
        version='version',
    )


def test_regster_activity_type_default_args(workflow_registrar, boto_client):
    workflow_registrar.register_activity_type(
        domain='domain',
        name='name',
        version='version',
    )

    boto_client.register_activity_type.assert_called_once_with(
        domain='domain',
        name='name',
        version='version',
    )


def test_register_workflow_type(workflow_registrar, boto_client):
    workflow_registrar.register_workflow_type(
        domain='domain',
        name='name',
        version='version',
        child_policy='child_policy',
        execution_start_to_close_timeout='execution_start_to_close_timeout',
        lambda_role='lambda_role',
        task_list_name='task_list_name',
        task_priority='task_priority',
        task_start_to_close_timeout='task_start_to_close_timeout',
        description='description',
    )

    boto_client.register_workflow_type.assert_called_once_with(
        defaultChildPolicy='child_policy',
        defaultExecutionStartToCloseTimeout='execution_start_to_close_timeout',
        defaultLambdaRole='lambda_role',
        defaultTaskList={
            'name': 'task_list_name',
        },
        defaultTaskPriority='task_priority',
        defaultTaskStartToCloseTimeout='task_start_to_close_timeout',
        description='description',
        domain='domain',
        name='name',
        version='version',
    )


def test_register_workflow_type_default(workflow_registrar, boto_client):
    workflow_registrar.register_workflow_type(
        domain='domain',
        name='name',
        version='version',
    )

    boto_client.register_workflow_type.assert_called_once_with(
        domain='domain',
        name='name',
        version='version',
    )
