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
    workflow_config.execution_start_to_close_timeout = 1
    boto_return = mock.MagicMock()
    boto_client.start_workflow_execution.return_value = boto_return
    workflow_name = 'test'
    version = '0.1'
    actual_run_id = workflow_client.start_workflow(
        input='meow',
        id='cat',
        workflow_name=workflow_name,
        version=version,
    )

    boto_client.start_workflow_execution.assert_called_once_with(
        domain=workflow_config.domain,
        childPolicy='TERMINATE',
        workflowId='cat',
        input='meow',
        workflowType={
            'name': workflow_name,
            'version': version,
        },
        taskList={
            'name': workflow_config.task_list,
        },
        executionStartToCloseTimeout=str(workflow_config.execution_start_to_close_timeout),
        taskStartToCloseTimeout=str(workflow_config.task_start_to_close_timeout),
    )
    assert actual_run_id == boto_return['runId']
    assert str(workflow_config.execution_start_to_close_timeout) ==\
        boto_client.start_workflow_execution.call_args[1]['executionStartToCloseTimeout']


def test_start_workflow_with_custom_execution_timeout(workflow_config, workflow_client, boto_client):
    boto_return = mock.MagicMock()
    boto_client.start_workflow_execution.return_value = boto_return
    workflow_name = 'test'
    version = '0.1'
    execution_timeout = 123
    workflow_client.start_workflow(
        input='meow',
        id='cat',
        workflow_name=workflow_name,
        version=version,
        workflow_start_to_close_timeout=execution_timeout,
    )

    boto_client.start_workflow_execution.assert_called_once_with(
        domain=workflow_config.domain,
        childPolicy='TERMINATE',
        workflowId='cat',
        input='meow',
        workflowType={
            'name': workflow_name,
            'version': version,
        },
        taskList={
            'name': workflow_config.task_list,
        },
        executionStartToCloseTimeout=str(execution_timeout),
        taskStartToCloseTimeout=str(workflow_config.task_start_to_close_timeout),
    )


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


def test_build_time_filter_dict_without_date():
    time_filter_dict = _build_time_filter_dict()
    assert time_filter_dict == {}


def test_build_time_filter_dict_with_oldest_start_date(oldest_start_date):
    time_filter_dict = _build_time_filter_dict(oldest_start_date=oldest_start_date)
    assert 'startTimeFilter' in time_filter_dict
    assert time_filter_dict['startTimeFilter'] == {'oldestDate': oldest_start_date}


def test_build_time_filter_dict_with_oldest_close_date(oldest_close_date):
    time_filter_dict = _build_time_filter_dict(oldest_close_date=oldest_close_date)
    assert 'closeTimeFilter' in time_filter_dict
    assert time_filter_dict['closeTimeFilter'] == {'oldestDate': oldest_close_date}


def test_build_time_filter_dict_with_start_date_range(oldest_start_date, latest_start_date):
    time_filter_dict = _build_time_filter_dict(oldest_start_date=oldest_start_date, latest_start_date=latest_start_date)
    assert 'startTimeFilter' in time_filter_dict
    assert time_filter_dict['startTimeFilter'] == {'oldestDate': oldest_start_date, 'latestDate': latest_start_date}


def test_build_time_filter_dict_with_close_date_range(oldest_close_date, latest_close_date):
    time_filter_dict = _build_time_filter_dict(oldest_close_date=oldest_close_date, latest_close_date=latest_close_date)
    assert 'closeTimeFilter' in time_filter_dict
    assert time_filter_dict['closeTimeFilter'] == {'oldestDate': oldest_close_date, 'latestDate': latest_close_date}


class TestCountOpenWorkflowExecutions:

    @pytest.fixture
    def mocked_boto_client(self):
        mocked_boto_client = mock.Mock()
        mocked_boto_client.count_open_workflow_executions.return_value = {
            'count': 123,
            'truncated': False
        }
        return mocked_boto_client

    @pytest.fixture
    def workflow_client(self, workflow_config, mocked_boto_client):
        return WorkflowClient(workflow_config, mocked_boto_client)

    def test_count_open_workflow_executions_with_oldest_start_time(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date
    ):
        response = workflow_client.count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
        )
        assert response.count == 123
        assert not response.truncated
        mocked_boto_client.count_open_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(oldestDate=oldest_start_date),
        )

    def test_count_open_workflow_executions_with_start_time_range(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date,
            latest_start_date
    ):
        workflow_client.count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            latest_start_date=latest_start_date,
        )
        mocked_boto_client.count_open_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(
                oldestDate=oldest_start_date,
                latestDate=latest_start_date,
            ),
        )

    def test_count_open_workflow_executions_with_oldest_start_time_and_workflow_name(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date,
    ):
        workflow_client.count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            workflow_name='test',
            version='test',
        )
        mocked_boto_client.count_open_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(oldestDate=oldest_start_date),
            typeFilter=dict(
                name='test',
                version='test',
            )
        )

    def test_count_open_workflow_executions_with_oldest_start_time_and_tag(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date,
    ):
        workflow_client.count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            tag='test',
        )
        mocked_boto_client.count_open_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(oldestDate=oldest_start_date),
            tagFilter=dict(tag='test'),
        )

    def test_count_open_workflow_executions_with_oldest_start_time_and_workflow_id(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date,
    ):
        workflow_client.count_open_workflow_executions(
            oldest_start_date=oldest_start_date,
            workflow_id='test'
        )
        mocked_boto_client.count_open_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(oldestDate=oldest_start_date),
            executionFilter=dict(workflowId='test'),
        )


class TestCountClosedWorkflowExecutions:

    @pytest.fixture
    def mocked_boto_client(self):
        mocked_boto_client = mock.Mock()
        mocked_boto_client.count_closed_workflow_executions.return_value = {
            'count': 321,
            'truncated': True
        }
        return mocked_boto_client

    @pytest.fixture
    def workflow_client(self, workflow_config, mocked_boto_client):
        return WorkflowClient(workflow_config, mocked_boto_client)

    def test_count_closed_workflow_executions_and_oldest_start_time(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date,
    ):
        response = workflow_client.count_closed_workflow_executions(oldest_start_date=oldest_start_date)
        assert response.count == 321
        assert response.truncated
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(oldestDate=oldest_start_date)
        )

    def test_count_closed_workflow_executions_with_start_time_range(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date,
            latest_start_date,
    ):
        workflow_client.count_closed_workflow_executions(oldest_start_date=oldest_start_date, latest_start_date=latest_start_date)
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(
                oldestDate=oldest_start_date,
                latestDate=latest_start_date,
            )
        )

    def test_count_closed_workflow_executions_with_oldest_close_time(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_close_date,
    ):
        workflow_client.count_closed_workflow_executions(oldest_close_date=oldest_close_date)
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            closeTimeFilter=dict(oldestDate=oldest_close_date),
        )

    def test_count_closed_workflow_executions_with_close_time_range(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_close_date,
            latest_close_date,
    ):
        workflow_client.count_closed_workflow_executions(oldest_close_date=oldest_close_date, latest_close_date=latest_close_date)
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            closeTimeFilter=dict(oldestDate=oldest_close_date, latestDate=latest_close_date),
        )

    def test_count_closed_workflow_executions_with_oldest_start_time_and_workflow_name(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date,
    ):
        workflow_client.count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            workflow_name='test',
            version='test',
        )
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(oldestDate=oldest_start_date),
            typeFilter=dict(
                name='test',
                version='test',
            ),
        )

    def test_count_closed_workflow_executions_with_oldest_close_time_and_workflow_name(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_close_date,
    ):
        workflow_client.count_closed_workflow_executions(
            oldest_close_date=oldest_close_date,
            workflow_name='test',
            version='test',
        )
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            closeTimeFilter=dict(oldestDate=oldest_close_date),
            typeFilter=dict(
                name='test',
                version='test',
            ),
        )

    def test_count_closed_workflow_executions_with_oldest_start_time_and_tag(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date,
    ):
        workflow_client.count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            tag='test',
        )
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(oldestDate=oldest_start_date),
            tagFilter=dict(tag='test'),
        )

    def test_count_closed_workflow_executions_with_oldest_close_time_and_tag(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_close_date,
    ):
        workflow_client.count_closed_workflow_executions(
            oldest_close_date=oldest_close_date,
            tag='test',
        )
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            closeTimeFilter=dict(oldestDate=oldest_close_date),
            tagFilter=dict(tag='test'),
        )

    def test_count_closed_workflow_executions_with_oldest_start_time_and_workflow_id(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date,
    ):
        workflow_client.count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            workflow_id='test',
        )
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(oldestDate=oldest_start_date),
            executionFilter=dict(workflowId='test'),
        )

    def test_count_closed_workflow_executions_with_oldest_close_time_and_workflow_id(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_close_date,
    ):
        workflow_client.count_closed_workflow_executions(
            oldest_close_date=oldest_close_date,
            workflow_id='test',
        )
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            closeTimeFilter=dict(oldestDate=oldest_close_date),
            executionFilter=dict(workflowId='test'),
        )

    def test_count_closed_workflow_executions_with_oldest_start_time_and_close_status(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_start_date,
    ):
        workflow_client.count_closed_workflow_executions(
            oldest_start_date=oldest_start_date,
            close_status='test',
        )
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            startTimeFilter=dict(oldestDate=oldest_start_date),
            closeStatusFilter=dict(status='test'),
        )

    def test_count_closed_workflow_executions_with_oldest_close_time_and_close_status(
            self,
            workflow_config,
            workflow_client,
            mocked_boto_client,
            oldest_close_date,
    ):
        workflow_client.count_closed_workflow_executions(
            oldest_close_date=oldest_close_date,
            close_status='test',
        )
        mocked_boto_client.count_closed_workflow_executions.assert_called_with(
            domain=workflow_config.domain,
            closeTimeFilter=dict(oldestDate=oldest_close_date),
            closeStatusFilter=dict(status='test'),
        )
