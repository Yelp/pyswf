# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import functools
import mock
import pytest

from datetime import datetime
from unittest import TestCase

from py_swf.clients.workflow import _build_time_filter_dict
from py_swf.clients.workflow import InvalidQueryToCountWorkflow
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


class AssertExceptionRaisedTestHelper(TestCase):
    def assertion_exception_raised(self, exception_class, func, *args, **kwargs):
        with self.assertRaises(exception_class):
            return func(*args, **kwargs)

    def assert_no_exception_raised(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.fail("unexpected exception {}".format(repr(e)))

    # unittest.TestCase requires runTest()
    def runTest(self):
        pass


def test_build_time_filter_dict():
    time_filter_dict = _build_time_filter_dict()
    assert time_filter_dict is None

    start_date = datetime(2016, 11, 11)
    time_filter_dict = _build_time_filter_dict(oldest_start_date=start_date)
    assert 'startTimeFilter' in time_filter_dict
    assert time_filter_dict['startTimeFilter'] == {'oldestDate': start_date}

    time_filter_dict = _build_time_filter_dict(oldest_close_date=start_date)
    assert 'closeTimeFilter' in time_filter_dict
    assert time_filter_dict['closeTimeFilter'] == {'oldestDate': start_date}

    end_date = datetime(2017, 11, 11)
    time_filter_dict = _build_time_filter_dict(oldest_start_date=start_date, latest_start_date=end_date)
    assert 'startTimeFilter' in time_filter_dict
    assert time_filter_dict['startTimeFilter'] == {'oldestDate': start_date, 'latestDate': end_date}

    time_filter_dict = _build_time_filter_dict(oldest_close_date=start_date, latest_close_date=end_date)
    assert 'closeTimeFilter' in time_filter_dict
    assert time_filter_dict['closeTimeFilter'] == {'oldestDate': start_date, 'latestDate': end_date}

    test_helper = AssertExceptionRaisedTestHelper()
    assert_invalid_datetime_query = functools.partial(
        test_helper.assertion_exception_raised,
        exception_class=InvalidQueryToCountWorkflow,
        func=_build_time_filter_dict,
    )
    assert_invalid_datetime_query(oldest_start_date=start_date, oldest_close_date=start_date)


def test_count_open_workflow_executions(workflow_config, workflow_client, boto_client):
    workflow_client._count_open_workflow_executions()
    _assert_count_open_workflow_executions_called_with(boto_client, workflow_config)

    oldest_start_date = datetime(2016, 11, 11)
    time_filter_dict = _build_time_filter_dict(oldest_start_date=oldest_start_date)
    workflow_client._count_open_workflow_executions(
        oldest_start_date=oldest_start_date,
    )
    _assert_count_open_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=time_filter_dict['startTimeFilter'],
    )

    workflow_filter_dict = {'test_filter': {'key': 'value'}}
    workflow_client._count_open_workflow_executions(
        oldest_start_date=oldest_start_date,
        workflow_filter_dict=workflow_filter_dict,
    )
    _assert_count_open_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=time_filter_dict['startTimeFilter'],
        test_filter=workflow_filter_dict['test_filter'],
    )


def test_count_closed_workflow_executions(workflow_config, workflow_client, boto_client):
    oldest_start_date = datetime(2016, 11, 11)
    oldest_close_date = oldest_start_date
    start_time_filter_dict = _build_time_filter_dict(oldest_start_date=oldest_start_date)
    close_time_filter_dict = _build_time_filter_dict(oldest_close_date=oldest_close_date)
    workflow_filter_dict = {'test_filter': {'key': 'value'}}

    workflow_client._count_closed_workflow_executions()
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
    )

    workflow_client._count_closed_workflow_executions(oldest_start_date=oldest_start_date)
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=start_time_filter_dict['startTimeFilter'],
    )

    workflow_client._count_closed_workflow_executions(oldest_close_date=oldest_close_date)
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        closeTimeFilter=close_time_filter_dict['closeTimeFilter'],
    )

    workflow_client._count_closed_workflow_executions(
        oldest_start_date=oldest_start_date,
        workflow_filter_dict=workflow_filter_dict,
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=start_time_filter_dict['startTimeFilter'],
        test_filter=workflow_filter_dict['test_filter'],
    )


def test_count_open_workflow_by_time(boto_client, workflow_client, workflow_config):
    oldest_start_date = datetime(2016, 11, 11)
    workflow_client.count_open_workflow_by_time(oldest_start_date=oldest_start_date)
    time_dict = dict(oldestDate=oldest_start_date)
    _assert_count_open_workflow_executions_called_with(boto_client, workflow_config, startTimeFilter=time_dict)

    latest_start_date = datetime(2016, 11, 24)
    workflow_client.count_open_workflow_by_time(
        oldest_start_date=oldest_start_date,
        latest_start_date=latest_start_date,
    )
    time_dict = dict(oldestDate=oldest_start_date, latestDate=latest_start_date)
    _assert_count_open_workflow_executions_called_with(boto_client, workflow_config, startTimeFilter=time_dict)


def _assert_count_open_workflow_executions_called_with(boto_client, workflow_config, **kwargs):
    _assert_boto_method_called_with(boto_client.count_open_workflow_executions, workflow_config.domain, **kwargs)


def _assert_count_closed_workflow_executions_called_with(boto_client, workflow_config, **kwargs):
    _assert_boto_method_called_with(boto_client.count_closed_workflow_executions, workflow_config.domain, **kwargs)


def _assert_boto_method_called_with(boto_method, domain, **kwargs):
    boto_method.assert_called_with(
        domain=domain,
        **kwargs
    )


def test_count_open_workflow_by_type(boto_client, workflow_client, workflow_config):
    name = 'test'
    version = 'test version'
    workflow_client.count_open_workflow_by_type(name=name)
    _assert_count_open_workflow_executions_called_with(boto_client, workflow_config, typeFilter=dict(name=name))

    workflow_client.count_open_workflow_by_type(name=name, version=version)
    _assert_count_open_workflow_executions_called_with(
        boto_client,
        workflow_config,
        typeFilter=dict(name=name, version=version),
    )


def test_count_open_workflow_by_tag(boto_client, workflow_client, workflow_config):
    tag = 'test tag'
    workflow_client.count_open_workflow_by_tag(tag=tag)
    _assert_count_open_workflow_executions_called_with(boto_client, workflow_config, tagFilter=dict(tag=tag))


def test_count_open_workflow_by_id(boto_client, workflow_client, workflow_config):
    workflow_id = 'test'
    workflow_client.count_open_workflow_by_id(workflow_id=workflow_id)
    _assert_count_open_workflow_executions_called_with(
        boto_client,
        workflow_config,
        executionFilter=dict(workflowId=workflow_id),
    )


def test_count_closed_workflow_by_time(boto_client, workflow_client, workflow_config):
    oldest_start_date = datetime(2016, 11, 11)
    latest_start_date = datetime(2016, 11, 24)
    oldest_close_date = datetime(2016, 12, 11)
    latest_close_date = datetime(2016, 12, 24)

    workflow_client.count_closed_workflow_by_time(oldest_start_date=oldest_start_date)
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date),
    )

    workflow_client.count_closed_workflow_by_time(
        oldest_start_date=oldest_start_date,
        latest_start_date=latest_start_date,
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        startTimeFilter=dict(oldestDate=oldest_start_date, latestDate=latest_start_date),
    )

    workflow_client.count_closed_workflow_by_time(oldest_close_date=oldest_close_date)
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        closeTimeFilter=dict(oldestDate=oldest_close_date),
    )

    workflow_client.count_closed_workflow_by_time(
        oldest_close_date=oldest_close_date,
        latest_close_date=latest_close_date,
    )
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        closeTimeFilter=dict(oldestDate=oldest_close_date, latestDate=latest_close_date),
    )

    test_helper = AssertExceptionRaisedTestHelper()
    assert_pass_mutally_exclusive_date = functools.partial(
        test_helper.assertion_exception_raised,
        exception_class=InvalidQueryToCountWorkflow,
        func=workflow_client.count_closed_workflow_by_time,
    )
    assert_pass_mutally_exclusive_date(oldest_start_date=oldest_start_date, oldest_close_date=oldest_close_date)


def test_count_closed_workflow_by_type(boto_client, workflow_client, workflow_config):
    name = 'test'
    version = 'test version'
    workflow_client.count_closed_workflow_by_type(name=name)
    _assert_count_closed_workflow_executions_called_with(boto_client, workflow_config, typeFilter=dict(name=name))

    workflow_client.count_closed_workflow_by_type(name=name, version=version)
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        typeFilter=dict(name=name, version=version),
    )


def test_count_closed_workflow_by_tag(boto_client, workflow_client, workflow_config):
    tag = 'test tag'
    workflow_client.count_closed_workflow_by_tag(tag=tag)
    _assert_count_closed_workflow_executions_called_with(boto_client, workflow_config, tagFilter=dict(tag=tag))


def test_count_closed_workflow_by_id(boto_client, workflow_client, workflow_config):
    workflow_id = 'test'
    workflow_client.count_closed_workflow_by_id(workflow_id=workflow_id)
    _assert_count_closed_workflow_executions_called_with(
        boto_client,
        workflow_config,
        executionFilter=dict(workflowId=workflow_id),
    )


def test_count_closed_workflow_by_close_status(boto_client, workflow_client, workflow_config):
    valid_status = set(['COMPLETED', 'FAILED', 'CANCELED', 'TERMINATED', 'CONTINUED_AS_NEW', 'TIMED_OUT'])
    for status in valid_status:
        workflow_client.count_closed_workflow_by_close_status(status)
        _assert_count_closed_workflow_executions_called_with(
            boto_client,
            workflow_config,
            closeStatusFilter=dict(status=status),
        )

    test_helper = AssertExceptionRaisedTestHelper()
    assert_invalid_close_state = functools.partial(
        test_helper.assertion_exception_raised,
        exception_class=InvalidQueryToCountWorkflow,
        func=workflow_client.count_closed_workflow_by_close_status,
    )
    assert_invalid_close_state(status='invalid status')
