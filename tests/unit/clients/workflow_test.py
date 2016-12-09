# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from datetime import datetime

from py_swf.clients.workflow import _build_time_filter_dict
from py_swf.clients.workflow import WorkflowClient


@pytest.fixture
def workflow_config():
    return mock.Mock()


@pytest.fixture
def workflow_client(workflow_config, boto_client):
    return WorkflowClient(workflow_config, boto_client)


@pytest.fixture
def oldest_start_date():
    return datetime(2016, 11, 11)


@pytest.fixture
def latest_start_date():
    return datetime(2016, 11, 12)


@pytest.fixture
def oldest_close_date():
    return datetime(2016, 11, 28)


@pytest.fixture
def latest_close_date():
    return datetime(2016, 11, 29)


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


def test_build_time_filter_dict(oldest_start_date, latest_start_date):
    time_filter_dict = _build_time_filter_dict()
    assert time_filter_dict is None

    start_date = oldest_start_date
    time_filter_dict = _build_time_filter_dict(oldest_start_date=start_date)
    assert 'startTimeFilter' in time_filter_dict
    assert time_filter_dict['startTimeFilter'] == {'oldestDate': start_date}

    time_filter_dict = _build_time_filter_dict(oldest_close_date=start_date)
    assert 'closeTimeFilter' in time_filter_dict
    assert time_filter_dict['closeTimeFilter'] == {'oldestDate': start_date}

    end_date = latest_start_date
    time_filter_dict = _build_time_filter_dict(oldest_start_date=start_date, latest_start_date=end_date)
    assert 'startTimeFilter' in time_filter_dict
    assert time_filter_dict['startTimeFilter'] == {'oldestDate': start_date, 'latestDate': end_date}

    time_filter_dict = _build_time_filter_dict(oldest_close_date=start_date, latest_close_date=end_date)
    assert 'closeTimeFilter' in time_filter_dict
    assert time_filter_dict['closeTimeFilter'] == {'oldestDate': start_date, 'latestDate': end_date}


def test_count_open_workflow_executions(workflow_config, workflow_client, boto_client, oldest_start_date, latest_start_date):
    workflow_client.count_open_workflow_executions(
        oldest_start_date=oldest_start_date,
    )
    _assert_count_open_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date),
    )

    workflow_client.count_open_workflow_executions(
        oldest_start_date=oldest_start_date,
        latest_start_date=latest_start_date,
    )
    _assert_count_open_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(
            oldestDate=oldest_start_date,
            latestDate=latest_start_date,
        ),
    )

    workflow_client.count_open_workflow_executions(
        oldest_start_date=oldest_start_date,
        workflow_name='test',
        version='test',
    )
    _assert_count_open_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date),
        typeFilter=dict(
            name='test',
            version='test',
        )
    )

    workflow_client.count_open_workflow_executions(
        oldest_start_date=oldest_start_date,
        tag='test',
    )
    _assert_count_open_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date),
        tagFilter=dict(tag='test'),
    )

    workflow_client.count_open_workflow_executions(
        oldest_start_date=oldest_start_date,
        workflow_id='test'
    )
    _assert_count_open_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date),
        executionFilter=dict(workflowId='test'),
    )


def test_count_closed_workflow_executions(
        workflow_config,
        workflow_client,
        boto_client,
        oldest_start_date,
        latest_start_date,
        oldest_close_date,
        latest_close_date,
):
    workflow_client.count_closed_workflow_executions(oldest_start_date=oldest_start_date)
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date)
    )

    workflow_client.count_closed_workflow_executions(oldest_start_date=oldest_start_date, latest_start_date=latest_start_date)
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(
            oldestDate=oldest_start_date,
            latestDate=latest_start_date,
        )
    )

    workflow_client.count_closed_workflow_executions(oldest_close_date=oldest_close_date)
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        closeTimeFilter=dict(oldestDate=oldest_close_date),
    )

    workflow_client.count_closed_workflow_executions(oldest_close_date=oldest_close_date, latest_close_date=latest_close_date)
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        closeTimeFilter=dict(oldestDate=oldest_close_date, latestDate=latest_close_date),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_start_date=oldest_start_date,
        workflow_name='test',
        version='test',
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date),
        typeFilter=dict(
            name='test',
            version='test',
        ),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_close_date=oldest_close_date,
        workflow_name='test',
        version='test',
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        closeTimeFilter=dict(oldestDate=oldest_close_date),
        typeFilter=dict(
            name='test',
            version='test',
        ),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_start_date=oldest_start_date,
        tag='test',
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date),
        tagFilter=dict(tag='test'),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_close_date=oldest_close_date,
        tag='test',
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        closeTimeFilter=dict(oldestDate=oldest_close_date),
        tagFilter=dict(tag='test'),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_start_date=oldest_start_date,
        workflow_id='test',
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date),
        executionFilter=dict(workflowId='test'),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_close_date=oldest_close_date,
        workflow_id='test',
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        closeTimeFilter=dict(oldestDate=oldest_close_date),
        executionFilter=dict(workflowId='test'),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_start_date=oldest_start_date,
        close_status='test',
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date),
        closeStatusFilter=dict(status='test'),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_close_date=oldest_close_date,
        close_status='test',
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        closeTimeFilter=dict(oldestDate=oldest_close_date),
        closeStatusFilter=dict(status='test'),
    )


def _assert_count_open_workflow_executions_called_with(boto_client, workflow_config, **kwargs):
    _assert_boto_method_called_with(boto_client.count_open_workflow_executions, workflow_config.domain, **kwargs)


def _assert_count_closed_workflow_executions_called_with(boto_client, workflow_config, **kwargs):
    _assert_boto_method_called_with(boto_client.count_closed_workflow_executions, workflow_config.domain, **kwargs)


def _assert_boto_method_called_with(boto_method, domain, **kwargs):
    boto_method.assert_called_with(
        domain=domain,
        **kwargs
    )
