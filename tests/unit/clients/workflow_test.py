# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from py_swf.clients.workflow import WorkflowClient


@pytest.fixture
def workflow_config():
    return mock.Mock()


@pytest.fixture
def workflow_client(workflow_config, boto_client):
    return WorkflowClient(workflow_config, boto_client)


def test_start_workflow(workflow_config, workflow_client, boto_client):
    boto_return = mock.MagicMock()
    boto_client.start_workflow_execution.return_value = boto_return
    actual_run_id = workflow_client.start_workflow(
        input='meow',
        id='cat',
    )

    boto_client.start_workflow_execution.assert_called_once_with(
        domain=workflow_config.domain,
        childPolicy='TERMINATE',
        workflowId='cat',
        input='meow',
        workflowType={
            'name': workflow_config.workflow_name,
            'version': workflow_config.workflow_version,
        },
        taskList={
            'name': workflow_config.task_list,
        },
        executionStartToCloseTimeout=str(workflow_config.execution_start_to_close_timeout),
        taskStartToCloseTimeout=str(workflow_config.task_start_to_close_timeout),
    )
    assert actual_run_id == boto_return['runId']


def test_terminate_workflow(workflow_config, workflow_client, boto_client):
    workflow_client.terminate_workflow(
        'workflow_id',
        'reason',
    )
    boto_client.terminate_workflow_execution.assert_called_once_with(
        domain=workflow_config.domain,
        workflowId='workflow_id',
        reason='reason',
    )
