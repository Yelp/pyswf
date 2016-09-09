# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from botocore.vendored.requests.exceptions import ReadTimeout

from pyswf.clients.activity_task import ActivityTask
from pyswf.clients.activity_task import ActivityTaskClient
from pyswf.errors import NoTaskFound
from testing.util import DictMock


@pytest.fixture
def activity_task_config():
    return mock.Mock()


@pytest.fixture
def activity_task_client(activity_task_config, boto_client):
    return ActivityTaskClient(
        activity_task_config,
        boto_client,
    )


class TestPolling:

    @pytest.fixture
    def boto_client_results(self):
        return DictMock()

    @pytest.fixture(autouse=True)
    def patch_poll_for_activity_task(self, boto_client_results, boto_client):
        boto_client.poll_for_activity_task.return_value = boto_client_results

    @pytest.fixture
    def expected_activity_task(self, boto_client_results):
        return ActivityTask(
            activity_id=boto_client_results['activityId'],
            type=boto_client_results['activityType']['name'],
            version=boto_client_results['activityType']['version'],
            input=boto_client_results['input'],
            task_token=boto_client_results['taskToken'],
            workflow_id=boto_client_results['workflowExecution']['workflowId'],
            workflow_run_id=boto_client_results['workflowExecution']['runId'],
        )

    def test_poll_with_identity(self, activity_task_config, expected_activity_task, activity_task_client, boto_client):
        result_activity_task = activity_task_client.poll(identity='meow')

        assert result_activity_task == expected_activity_task

        boto_client.poll_for_activity_task.assert_called_once_with(
            domain=activity_task_config.domain,
            identity=u'meow',
            taskList={
                u'name': activity_task_config.task_list,
            },
        )

    def test_poll_without_identity(self, activity_task_config, expected_activity_task, activity_task_client, boto_client):
        result_activity_task = activity_task_client.poll()

        assert result_activity_task == expected_activity_task

        boto_client.poll_for_activity_task.assert_called_once_with(
            domain=activity_task_config.domain,
            taskList={
                u'name': activity_task_config.task_list,
            },
        )

    def test_poll_timeout(self, activity_task_client, boto_client):
        boto_client.poll_for_activity_task.side_effect = ReadTimeout
        with pytest.raises(NoTaskFound):
            activity_task_client.poll()


class TestPollingWithBadResults:
    @pytest.fixture(autouse=True)
    def patch_poll_for_activity_task(self, boto_client_faulty_results, boto_client):
        boto_client.poll_for_activity_task.return_value = boto_client_faulty_results

    @pytest.fixture
    def boto_client_faulty_results(self):
        return {
            "startedEventId": 0,
            "previousStartedEventId": 0,
            "ResponseMetadata": {
                'HTTPStatusCode': 200,
                'RequestId': 'b1cde981-23a0-11e6-98d4-5316d1b97440'
            }
        }

    def test_receive_result_without_task_token(self, activity_task_client):
        with pytest.raises(NoTaskFound):
            activity_task_client.poll()


def test_finish(activity_task_client, boto_client):
    task_token = mock.Mock()
    result = mock.Mock()
    activity_task_client.finish(task_token, result)

    boto_client.respond_activity_task_completed.assert_called_once_with(
        result=result,
        taskToken=task_token,
    )


def test_fail_with_detail(activity_task_client, boto_client):
    task_token = mock.Mock()
    reason = "test_fail"
    detail = "detail"
    activity_task_client.fail(task_token, reason, detail)

    boto_client.respond_activity_task_failed.assert_called_once_with(
        details=detail,
        reason=reason,
        taskToken=task_token,
    )


def test_fail_without_detail(activity_task_client, boto_client):
    task_token = mock.Mock()
    reason = "test_fail"
    activity_task_client.fail(task_token, reason)

    boto_client.respond_activity_task_failed.assert_called_once_with(
        reason=reason,
        taskToken=task_token,
    )
