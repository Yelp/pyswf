# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

import mock
import pytest
from botocore.vendored.requests.exceptions import ReadTimeout

from py_swf.clients.decision import DecisionClient
from py_swf.clients.decision import DecisionTask
from py_swf.clients.decision import nametuplefy
from py_swf.errors import NoTaskFound
from testing.util import DictMock


class TestNametuplefy:

    def test_normal(self):
        dictionary = dict(
            cat='meow',
            dog='woof',
            hamster='???????',
        )
        result = nametuplefy(dictionary)
        assert result._asdict() == dictionary

    def test_empty(self):
        result = nametuplefy({})
        assert result._asdict() == {}

    def test_nested(self):
        dictionary = dict(
            thing=dict(
                other='Thing',
            ),
        )
        Outer = namedtuple('Dict', 'thing')
        Inner = namedtuple('Dict', 'other')
        expected = Outer(thing=Inner(other='Thing'))
        result = nametuplefy(dictionary)
        assert result == expected

    def test_list(self):
        dictionary = dict(
            animals=[
                dict(
                    kind='cat',
                ),
                dict(
                    kind='dog',
                ),
            ],
        )
        Outer = namedtuple('Dict', 'animals')
        Inner = namedtuple('Dict', 'kind')
        expected = Outer(
            animals=[
                Inner(kind='cat'),
                Inner(kind='dog'),
            ],
        )
        result = nametuplefy(dictionary)
        assert result == expected


@pytest.fixture
def decision_config():
    return mock.Mock()


@pytest.fixture
def decision_client(decision_config, boto_client):
    return DecisionClient(decision_config, boto_client)


class TestPolling:

    @pytest.fixture(autouse=True)
    def patch_poll_for_decision_task(self, boto_client_results, boto_client):
        boto_client.poll_for_decision_task.return_value = boto_client_results

    @pytest.fixture
    def boto_client_results(self, raw_decision_events):
        dict_mock = DictMock()
        dict_mock['events'] = raw_decision_events
        return dict_mock

    @pytest.fixture
    def raw_decision_events(self):
        return [
            {
                "eventId": 3,
                "decisionTaskStartedEventAttributes": {
                    "scheduledEventId": 2,
                    "identity": "Decider01"
                },
                "eventTimestamp": 1326593394.566,
                "eventType": "DecisionTaskStarted"
            },
            {
                "eventId": 2,
                "eventType": "DecisionTaskScheduled",
                "decisionTaskScheduledEventAttributes": {
                    "startToCloseTimeout": "600",
                    "taskList": {
                        "name": "specialTaskList"
                    },
                    "taskPriority": "100"
                },
                "eventTimestamp": 1326592619.474
            },
            {
                "eventId": 1,
                "eventType": "WorkflowExecutionStarted",
                "workflowExecutionStartedEventAttributes": {
                    "taskList": {
                        "name": "specialTaskList"
                    },
                    "parentInitiatedEventId": 0,
                    "taskStartToCloseTimeout": "600",
                    "childPolicy": "TERMINATE",
                    "executionStartToCloseTimeout": "3600",
                    "input": "arbitrary-string-that-is-meaningful-to-the-workflow",
                    "workflowType": {
                        "version": "1.0",
                        "name": "customerOrderWorkflow"
                    },
                    "tagList": [
                        "music purchase",
                        "digital",
                        "ricoh-the-dog"
                    ]
                },
                "eventTimestamp": 1326592619.474
            },
        ]

    @pytest.fixture
    def expected_decision_task(self, boto_client_results):
        return DecisionTask(
            events=nametuplefy(boto_client_results['events']),
            task_token=boto_client_results['taskToken'],
            workflow_id=boto_client_results['workflowExecution']['workflowId'],
            workflow_run_id=boto_client_results['workflowExecution']['runId'],
            workflow_type=boto_client_results['workflowType'],
        )

    def test_with_identity(self, decision_config, decision_client, expected_decision_task, boto_client):
        result_decision_task = decision_client.poll(identity='meow')

        assert result_decision_task == expected_decision_task

        boto_client.poll_for_decision_task.assert_called_once_with(
            domain=decision_config.domain,
            reverseOrder=True,
            taskList={
                'name': decision_config.task_list,
            },
            identity='meow',
        )

    def test_without_identity(self, decision_config, decision_client, expected_decision_task, boto_client):
        result_decision_task = decision_client.poll()

        assert result_decision_task == expected_decision_task

        boto_client.poll_for_decision_task.assert_called_once_with(
            domain=decision_config.domain,
            reverseOrder=True,
            taskList={
                'name': decision_config.task_list,
            },
        )

    def test_without_use_raw_event_history(self, decision_client, expected_decision_task):
        result_decision_task = decision_client.poll(use_raw_event_history=False)

        assert result_decision_task == expected_decision_task

    def test_with_use_raw_event_history(self, decision_client, expected_decision_task, raw_decision_events):
        result_decision_task = decision_client.poll(use_raw_event_history=True)

        expected_decision_task = expected_decision_task._replace(events=raw_decision_events)
        assert result_decision_task == expected_decision_task

    def test_poll_timeout(self, decision_client, boto_client):
        boto_client.poll_for_decision_task.side_effect = ReadTimeout
        with pytest.raises(NoTaskFound):
            decision_client.poll()


class TestPollingWithBadResults:
    @pytest.fixture(autouse=True)
    def patch_poll_for_decision_task(self, boto_client_faulty_results, boto_client):
        boto_client.poll_for_decision_task.return_value = boto_client_faulty_results

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

    def test_receive_result_without_task_token(self, decision_client):
        with pytest.raises(NoTaskFound):
            decision_client.poll()


def test_finish_decision_with_activity(decision_client, decision_config, boto_client):
    decision_client.finish_decision_with_activity(
        'task_token',
        'activity_id',
        'activity_name',
        'activity_version',
        'activity_input',
        {},
    )

    boto_client.respond_decision_task_completed.assert_called_once_with(
        taskToken='task_token',
        # We rely on acceptence test for the schema of decisions
        decisions=[mock.ANY],
    )


class TestWalkWorkflowExecutionHistory:

    def verify_next_value_in_execution_history(
        self,
        boto_client,
        execution_history,
        expected_result,
        expected_domain,
        expected_next_page_token,
        expected_workflow_id,
        expected_workflow_run_id,
    ):
        """Grabs the next element in the workflow history generator and asserts the previous invocation of
        get_workflow_execution_history had expected arguments, and asserts the next element in the workflow execution
        was expected.
        """
        actual_result = next(execution_history)

        assert actual_result == expected_result

        to_assert = dict(
            domain=expected_domain,
            reverseOrder=True,
            execution=dict(
                workflowId=expected_workflow_id,
                runId=expected_workflow_run_id,
            ),
            maximumPageSize=1000,
        )

        if expected_next_page_token is not None:
            to_assert['nextPageToken'] = expected_next_page_token

        boto_client.get_workflow_execution_history.assert_called_with(
            **to_assert
        )

    def mock_result_for_next_history_page(self, boto_client, new_events, new_next_page_token):
        """Mocks the result for get_workflow_execution_history, which is only called on successive page retrievals.
        """
        new_return_value = dict(
            events=new_events,
        )

        if new_next_page_token is not None:
            new_return_value['nextPageToken'] = new_next_page_token

        boto_client.get_workflow_execution_history.return_value = new_return_value

    def test_walk_execution_history_empty(self, decision_client, boto_client, decision_config):
        execution_history = decision_client.walk_execution_history(
            workflow_id='workflow_id',
            workflow_run_id='workflow_run_id',
        )
        self.mock_result_for_next_history_page(
            boto_client=boto_client,
            new_events=[],
            new_next_page_token=None,
        )
        with pytest.raises(StopIteration):
            next(execution_history)

    def test_walk_execution_history_paginated(self, decision_client, boto_client, decision_config):
        execution_history = decision_client.walk_execution_history(
            workflow_id='workflow_id',
            workflow_run_id='workflow_run_id',
        )
        self.mock_result_for_next_history_page(
            boto_client=boto_client,
            new_events=[1],
            new_next_page_token='token1',
        )

        self.verify_next_value_in_execution_history(
            boto_client=boto_client,
            execution_history=execution_history,
            expected_result=1,
            expected_next_page_token=None,
            expected_domain=decision_config.domain,
            expected_workflow_id='workflow_id',
            expected_workflow_run_id='workflow_run_id',
        )
        self.mock_result_for_next_history_page(
            boto_client=boto_client,
            new_events=[2],
            new_next_page_token=None,
        )

        self.verify_next_value_in_execution_history(
            boto_client=boto_client,
            execution_history=execution_history,
            expected_result=2,
            expected_next_page_token='token1',
            expected_domain=decision_config.domain,
            expected_workflow_id='workflow_id',
            expected_workflow_run_id='workflow_run_id',
        )
        with pytest.raises(StopIteration):
            next(execution_history)

    def test_without_use_raw_event_history(self, decision_client, boto_client):
        dictionary = dict(blah='meow', meow='blah')
        execution_history = decision_client.walk_execution_history(
            workflow_id='workflow_id',
            workflow_run_id='workflow_run_id',
        )
        self.mock_result_for_next_history_page(
            boto_client=boto_client,
            new_events=[dictionary],
            new_next_page_token=None,
        )
        result = next(execution_history)
        assert result == nametuplefy(dictionary)

    def test_with_use_raw_event_history(self, decision_client, boto_client):
        dictionary = dict(blah='meow', meow='blah')
        execution_history = decision_client.walk_execution_history(
            workflow_id='workflow_id',
            workflow_run_id='workflow_run_id',
            use_raw_event_history=True,
        )
        self.mock_result_for_next_history_page(
            boto_client=boto_client,
            new_events=[dictionary],
            new_next_page_token=None,
        )
        result = next(execution_history)
        assert result == dictionary
