# -*- coding: utf-8 -*-
"""Smoke tests that demonstrate we can connect to SWF and make rudimentary calls. Also
provides examples on how to use this library.
"""
from __future__ import absolute_import
from __future__ import unicode_literals
from botocore.exceptions import ClientError
from unittest import TestCase

import uuid

from datetime import datetime
import pytest
import simplejson


# Expect these tests to fail due to not having credentials for Amazon SWF
pytestmark = pytest.mark.xfail


def start_workflow(workflow_client, workflow_id, workflow_start_to_close_timeout=None):
    return workflow_client.start_workflow(
        input=simplejson.dumps(dict(
            cat=True,
            meow='yes',
        )),
        id=workflow_id,
        workflow_name='test',
        version='0.1',
        workflow_start_to_close_timeout=workflow_start_to_close_timeout,
    )


def poll_decision_and_respond(decision_client, response_override_dict=None):
    decision_task = decision_client.poll()

    activity_id = str(uuid.uuid4())
    activity_input = dict(
        dog=True,
        woof='meow',
    )
    decision_client.finish_decision_with_activity(
        task_token=decision_task.task_token,
        activity_id=activity_id,
        activity_name='test_activity',
        activity_version='0.1',
        activity_input=simplejson.dumps(activity_input),
        override_config_dict=response_override_dict,
    )
    return decision_task


def poll_activity_task_and_respond(activity_task_client):
    activity_task = activity_task_client.poll()
    result = dict(
        hamsters=True,
        cute='very yes',
    )
    activity_task_client.finish(
        task_token=activity_task.task_token,
        result=simplejson.dumps(result),
    )
    return activity_task


def poll_activity_task_and_respond_failed(activity_task_client):
    activity_task = activity_task_client.poll()
    activity_task_client.fail(
        task_token=activity_task.task_token,
        reason='test fail',
    )
    return activity_task


def terminate_workflow(workflow_client, workflow_id):
    return workflow_client.terminate_workflow(workflow_id, 'meowowow')


def poll_decision_and_finish_workflow(decision_client, workflow_client):
    decision_task = decision_client.poll()
    decision_client.finish_workflow(decision_task.task_token, 'meowed')
    return decision_task


def walk_execution_history(decision_client, workflow_id, workflow_run_id):
    return list(decision_client.walk_execution_history(
        workflow_id,
        workflow_run_id,
        maximum_page_size=1,  # Force pagination
    ))


def test_workflow(workflow_client, decision_client, activity_task_client):
    workflow_id = str(uuid.uuid4())
    start_workflow(workflow_client, workflow_id)
    poll_decision_and_respond(decision_client)
    poll_activity_task_and_respond(activity_task_client)
    poll_decision_and_finish_workflow(decision_client, workflow_client)


def test_workflow_with_start_to_close_timeout(workflow_client, decision_client, activity_task_client):
    workflow_id = str(uuid.uuid4())
    start_workflow(workflow_client, workflow_id, workflow_start_to_close_timeout=60)
    poll_decision_and_respond(decision_client)
    poll_activity_task_and_respond(activity_task_client)
    poll_decision_and_finish_workflow(decision_client, workflow_client)


def test_workflow_with_activity_timeout(workflow_client, decision_client, activity_task_client):
    workflow_id = str(uuid.uuid4())
    start_workflow(workflow_client, workflow_id, workflow_start_to_close_timeout=100)
    override_dict = {
        'schedule_to_close_timeout': 30,
        'schedule_to_start_timeout': 10,
        'start_to_close_timeout': 20,
    }
    poll_decision_and_respond(decision_client, response_override_dict=override_dict)
    poll_activity_task_and_respond(activity_task_client)
    poll_decision_and_finish_workflow(decision_client, workflow_client)


def test_workflow_with_activity_failed(workflow_client, decision_client, activity_task_client):
    workflow_id = str(uuid.uuid4())
    start_workflow(workflow_client, workflow_id)
    poll_decision_and_respond(decision_client)
    poll_activity_task_and_respond_failed(activity_task_client)
    poll_decision_and_finish_workflow(decision_client, workflow_client)


def test_terminate_workflow(workflow_client, decision_client):
    workflow_id = str(uuid.uuid4())
    start_workflow(workflow_client, workflow_id)
    poll_decision_and_respond(decision_client)
    terminate_workflow(workflow_client, workflow_id)


def test_walk_execution_history(workflow_client, decision_client):
    workflow_id = str(uuid.uuid4())
    start_workflow(workflow_client, workflow_id)
    decision_task = poll_decision_and_respond(decision_client)
    terminate_workflow(workflow_client, workflow_id)
    walk_execution_history(decision_client, workflow_id, decision_task.workflow_run_id)


def test_count_open_workflow_executions(workflow_client):
    workflow_client.count_open_workflow_executions(
        oldest_start_date=datetime(2016, 11, 11),
    )

    workflow_client.count_open_workflow_executions(
        oldest_start_date=datetime(2016, 11, 11),
        latest_start_date=datetime(2016, 11, 12),
    )

    workflow_client.count_open_workflow_executions(
        oldest_start_date=datetime(2016, 11, 11),
        workflow_id=str(uuid.uuid4()),
    )

    workflow_client.count_open_workflow_executions(
        oldest_start_date=datetime(2016, 11, 11),
        workflow_name='workflow',
        version='1.0'
    )

    workflow_client.count_open_workflow_executions(
        oldest_start_date=datetime(2016, 11, 11),
        tag='tag'
    )


class ExceptionAssertionHelper(TestCase):
    def __init__(self, excpt, func):
        self.test_method = func
        self.excpetion_type = excpt

    def run_test(self, *args, **kwargs):
        self.assertRaises(self.excpetion_type, self.test_method, *args, **kwargs)


def test_count_closed_workflow_executions(workflow_client):
    workflow_client.count_closed_workflow_executions(
        oldest_start_date=datetime(2016, 11, 11),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_start_date=datetime(2016, 11, 11),
        latest_start_date=datetime(2016, 11, 12),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_close_date=datetime(2016, 11, 12),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_close_date=datetime(2016, 11, 12),
        latest_close_date=datetime(2016, 11, 13),
    )

    invalid_date_test_helper = ExceptionAssertionHelper(ClientError, workflow_client.count_closed_workflow_executions)
    invalid_date_test_helper.run_test(
        oldest_start_date=datetime(2016, 11, 11),
        latest_start_date=datetime(2016, 11, 12),
        oldest_close_date=datetime(2016, 11, 12),
        latest_close_date=datetime(2016, 11, 13),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_start_date=datetime(2016, 11, 11),
        workflow_name='workflow',
        version='1.0'
    )

    workflow_client.count_closed_workflow_executions(
        oldest_close_date=datetime(2016, 11, 11),
        tag='tag',
    )

    workflow_client.count_closed_workflow_executions(
        oldest_start_date=datetime(2016, 11, 11),
        workflow_id=str(uuid.uuid4()),
    )

    workflow_client.count_closed_workflow_executions(
        oldest_start_date=datetime(2016, 11, 11),
        close_status='COMPLETED',
    )

    invalid_workflow_filter_test_helper = ExceptionAssertionHelper(ClientError, workflow_client.count_closed_workflow_executions)
    invalid_workflow_filter_test_helper.run_test(
        oldest_start_date=datetime(2016, 11, 11),
        latest_start_date=datetime(2016, 11, 12),
        workflow_name='workflow',
        tag='tag',
        workflow_id=str(uuid.uuid4()),
        close_status='COMPLETED',
    )
