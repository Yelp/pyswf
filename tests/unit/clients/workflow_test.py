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


@pytest.fixture
def start_time_filter_with_oldest_date(oldest_start_date):
    return dict(startTimeFilter=dict(oldestDate=oldest_start_date))


@pytest.fixture
def start_time_filter_with_both_date(oldest_start_date, latest_start_date):
    return dict(
        startTimeFilter=dict(
            oldestDate=oldest_start_date,
            latestDate=latest_start_date,
        )
    )


@pytest.fixture
def close_time_filter_with_oldest_date(oldest_close_date):
    return dict(closeTimeFilter=dict(oldestDate=oldest_close_date))


@pytest.fixture
def close_time_filter_with_both_date(oldest_close_date, latest_close_date):
    return dict(
        closeTimeFilter=dict(
            oldestDate=oldest_close_date,
            latestDate=latest_close_date,
        )
    )


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


def test_count_open_workflow_executions(workflow_config, workflow_client, boto_client, oldest_start_date):
    workflow_client._count_open_workflow_executions()
    _assert_count_open_workflow_executions_called_with(boto_client, workflow_config)

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


def test_count_closed_workflow_executions(workflow_config, workflow_client, boto_client, oldest_start_date, oldest_close_date):
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


def _assert_count_open_workflow_executions_called_with(boto_client, workflow_config, **kwargs):
    _assert_boto_method_called_with(boto_client.count_open_workflow_executions, workflow_config.domain, **kwargs)


def _assert_count_closed_workflow_executions_called_with(boto_client, workflow_config, **kwargs):
    _assert_boto_method_called_with(boto_client.count_closed_workflow_executions, workflow_config.domain, **kwargs)


def _assert_boto_method_called_with(boto_method, domain, **kwargs):
    boto_method.assert_called_with(
        domain=domain,
        **kwargs
    )


def _count_open_workflow_test_helper(boto_client, workflow_config, test_method, test_kwargs_list):
    for kwargs_tuple in test_kwargs_list:
        test_kwargs, expected_kwargs = kwargs_tuple
        test_method(**test_kwargs)
        _assert_count_open_workflow_executions_called_with(boto_client, workflow_config, **expected_kwargs)


@pytest.fixture
def count_by_start_time_kwarg_list(
        oldest_start_date,
        latest_start_date,
        start_time_filter_with_oldest_date,
        start_time_filter_with_both_date
):
    return [
        (dict(oldest_start_date=oldest_start_date), start_time_filter_with_oldest_date),
        (dict(oldest_start_date=oldest_start_date, latest_start_date=latest_start_date), start_time_filter_with_both_date),
    ]


def test_count_open_workflow_by_start_time(boto_client, workflow_client, workflow_config, count_by_start_time_kwarg_list):
    _count_open_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_open_workflow_by_start_time,
        count_by_start_time_kwarg_list
    )


@pytest.fixture
def count_by_type_and_start_time_kwarg_list(oldest_start_date, start_time_filter_with_oldest_date):
    return [
        (
            dict(name='test', version='test version', oldest_start_date=oldest_start_date),
            dict(typeFilter=dict(name='test', version='test version'), **start_time_filter_with_oldest_date)
        )
    ]


def test_count_open_workflow_by_type_and_start_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_type_and_start_time_kwarg_list
):
    _count_open_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_open_workflow_by_type_and_start_time,
        count_by_type_and_start_time_kwarg_list
    )


@pytest.fixture
def count_by_tag_and_start_time_kwarg_list(oldest_start_date, start_time_filter_with_oldest_date):
    return [
        (
            dict(tag='test', oldest_start_date=oldest_start_date),
            dict(tagFilter=dict(tag='test'), **start_time_filter_with_oldest_date)
        )
    ]


def test_count_open_workflow_by_tag_and_start_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_tag_and_start_time_kwarg_list
):
    _count_open_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_open_workflow_by_tag_and_start_time,
        count_by_tag_and_start_time_kwarg_list
    )


@pytest.fixture
def count_by_id_and_start_time_kwarg_list(oldest_start_date, start_time_filter_with_oldest_date):
    return [
        (
            dict(workflow_id='test', oldest_start_date=oldest_start_date),
            dict(executionFilter=dict(workflowId='test'), **start_time_filter_with_oldest_date)
        ),
    ]


def test_count_open_workflow_by_id_and_start_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_id_and_start_time_kwarg_list
):
    _count_open_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_open_workflow_by_id_and_start_time,
        count_by_id_and_start_time_kwarg_list
    )


def _count_closed_workflow_test_helper(boto_client, workflow_config, test_method, test_kwargs_list):
    for kwargs_tuple in test_kwargs_list:
        test_kwargs, expected_kwargs = kwargs_tuple
        test_method(**test_kwargs)
        _assert_count_closed_workflow_executions_called_with(boto_client, workflow_config, **expected_kwargs)


def test_count_closed_workflow_by_start_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_start_time_kwarg_list
):
    _count_closed_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_closed_workflow_by_start_time,
        count_by_start_time_kwarg_list
    )


@pytest.fixture
def count_by_close_time_kwarg_list(
        oldest_close_date,
        latest_close_date,
        close_time_filter_with_oldest_date,
        close_time_filter_with_both_date
):
    return [
        (dict(oldest_close_date=oldest_close_date), close_time_filter_with_oldest_date),
        (dict(oldest_close_date=oldest_close_date, latest_close_date=latest_close_date), close_time_filter_with_both_date),
    ]


def test_count_closed_workflow_by_close_time(boto_client, workflow_client, workflow_config, count_by_close_time_kwarg_list):
    _count_closed_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_closed_workflow_by_close_time,
        count_by_close_time_kwarg_list
    )


def test_count_closed_workflow_by_type_and_start_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_type_and_start_time_kwarg_list
):
    _count_closed_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_closed_workflow_by_type_and_start_time,
        count_by_type_and_start_time_kwarg_list
    )


@pytest.fixture
def count_by_type_and_close_time_kwarg_list(oldest_close_date, close_time_filter_with_oldest_date):
    return [
        (
            dict(name='test', version='test version', oldest_close_date=oldest_close_date),
            dict(typeFilter=dict(name='test', version='test version'), **close_time_filter_with_oldest_date)
        )
    ]


def test_count_closed_workflow_by_type_and_close_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_type_and_close_time_kwarg_list
):
    _count_closed_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_closed_workflow_by_type_and_close_time,
        count_by_type_and_close_time_kwarg_list
    )


def test_count_closed_workflow_by_tag_and_start_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_tag_and_start_time_kwarg_list
):
    _count_closed_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_closed_workflow_by_tag_and_start_time,
        count_by_tag_and_start_time_kwarg_list
    )


@pytest.fixture
def count_by_tag_and_close_time_kwarg_list(oldest_close_date, close_time_filter_with_oldest_date):
    return [
        (
            dict(tag='test', oldest_close_date=oldest_close_date),
            dict(tagFilter=dict(tag='test'), **close_time_filter_with_oldest_date)
        )
    ]


def test_count_closed_workflow_by_tag_and_close_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_tag_and_close_time_kwarg_list
):
    _count_closed_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_closed_workflow_by_tag_and_close_time,
        count_by_tag_and_close_time_kwarg_list
    )


def test_count_closed_workflow_by_id_and_start_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_id_and_start_time_kwarg_list
):
    _count_closed_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_closed_workflow_by_id_and_start_time,
        count_by_id_and_start_time_kwarg_list
    )


@pytest.fixture
def count_by_id_and_close_time_kwarg_list(oldest_close_date, close_time_filter_with_oldest_date):
    return [
        (
            dict(workflow_id='test', oldest_close_date=oldest_close_date),
            dict(executionFilter=dict(workflowId='test'), **close_time_filter_with_oldest_date)
        )
    ]


def test_count_closed_workflow_by_id_and_close_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_id_and_close_time_kwarg_list
):
    _count_closed_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_closed_workflow_by_id_and_close_time,
        count_by_id_and_close_time_kwarg_list
    )


@pytest.fixture
def count_by_close_status_kwarg_list(
        oldest_start_date=None,
        start_time_filter_with_oldest_date=None,
        oldest_close_date=None,
        close_time_filter_with_oldest_date=None
):
    valid_status = set(['COMPLETED', 'FAILED', 'CANCELED', 'TERMINATED', 'CONTINUED_AS_NEW', 'TIMED_OUT'])
    kwarg_list = []
    for status in valid_status:
        test_kwarg = dict(status=status)
        expected_kwarg = dict(closeStatusFilter=dict(status=status))
        if oldest_start_date:
            test_kwarg['oldest_start_date'] = oldest_start_date
            expected_kwarg.update(start_time_filter_with_oldest_date)
        elif oldest_close_date:
            test_kwarg['oldest_close_date'] = oldest_close_date
            expected_kwarg.update(close_time_filter_with_oldest_date)
        kwarg_list.append((test_kwarg, expected_kwarg))
    return kwarg_list


@pytest.fixture
def count_by_close_status_and_start_time_kwarg_list(oldest_start_date, start_time_filter_with_oldest_date):
    return count_by_close_status_kwarg_list(
        oldest_start_date=oldest_start_date,
        start_time_filter_with_oldest_date=start_time_filter_with_oldest_date
    )


def test_count_closed_workflow_by_close_status_and_start_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_close_status_and_start_time_kwarg_list,
):
    _count_closed_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_closed_workflow_by_close_status_and_start_time,
        count_by_close_status_and_start_time_kwarg_list
    )


@pytest.fixture
def count_by_close_status_and_close_time_kwarg_list(oldest_close_date, close_time_filter_with_oldest_date):
    return count_by_close_status_kwarg_list(
        oldest_close_date=oldest_close_date,
        close_time_filter_with_oldest_date=close_time_filter_with_oldest_date
    )


def test_count_closed_workflow_by_close_status_and_close_time(
        boto_client,
        workflow_client,
        workflow_config,
        count_by_close_status_and_close_time_kwarg_list,
):
    _count_closed_workflow_test_helper(
        boto_client,
        workflow_config,
        workflow_client.count_closed_workflow_by_close_status_and_close_time,
        count_by_close_status_and_close_time_kwarg_list
    )
