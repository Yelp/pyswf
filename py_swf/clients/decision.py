# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from botocore.vendored.requests.exceptions import ReadTimeout

from py_swf.errors import NoTaskFound


__all__ = ['DecisionClient', 'DecisionTask']


DecisionTask = namedtuple('DecisionTask', 'events task_token workflow_id workflow_run_id workflow_type')
"""Contains the metadata to execute a decision task.

See the response syntax in :meth:`~SWF.Client.poll_for_decision_task`.
"""


def nametuplefy(thing):
    """Recursively turns a dict into namedtuples."""
    if type(thing) == dict:
        # Only supports string keys
        Dict = namedtuple('Dict', ' '.join(thing.keys()))

        nametuplefied_children = {}

        for k, v in thing.items():
            nametuplefied_children[k] = nametuplefy(v)

        return Dict(**nametuplefied_children)
    if type(thing) == list:
        return list(map(nametuplefy, thing))
    else:
        return thing


class DecisionClient(object):
    """A client that provides a pythonic API for polling and responding to decision tasks through an SWF boto3 client.

    :param decision_config: Contains SWF values commonly used when making SWF api calls.
    :type decision_config: :class:`~py_swf.config_definitions.DecisionConfig`
    :param boto_client: A raw SWF boto3 client.
    :type boto_client: :class:`~SWF.Client`
    """

    def __init__(self, decision_config, boto_client):
        self.decision_config = decision_config
        self.boto_client = boto_client

    def poll(self, identity=None, use_raw_event_history=False):
        """Opens a connection to AWS and long-polls for decision tasks.
        When a decision is available, this function will return with exactly one decision task to execute.
        Only returns a contiguous subset of the most recent events.
        If you want to grab the entire history for a workflow, use :meth:`~py_swf.decision.DecisionClient.walk_execution_history`

        Passthrough to :meth:`~SWF.Client.poll_for_decision_task`.

        :param identity: A freeform text that identifies the client that performed the longpoll. Useful for debugging history.
        :type identity: string
        :param use_raw_event_history: Whether to use the raw dictionary event history returned from AWS.
                                      Otherwise attempts to turn dictionaries into namedtuples recursively.
        :type use_raw_event_history: bool
        :return: A decision task to execute.
        :rtype: DecisionTask
        :raises py_swf.errors.NoTaskFound: Raised when polling for a decision task times out without receiving any tasks.
        """
        kwargs = dict(
            domain=self.decision_config.domain,
            reverseOrder=True,
            taskList={
                'name': self.decision_config.task_list,
            },
        )

        # boto doesn't like None values for optional kwargs
        if identity is not None:
            kwargs['identity'] = identity

        try:
            results = self.boto_client.poll_for_decision_task(
                **kwargs
            )
        except ReadTimeout as e:
            raise NoTaskFound(e)

        # Sometimes SWF gives us an incomplete response, ignore these.
        if not results.get('taskToken', None):
            raise NoTaskFound('Received results with no taskToken')

        events = results['events']
        if not use_raw_event_history:
            events = nametuplefy(events)

        return DecisionTask(
            events=events,
            task_token=results['taskToken'],
            workflow_id=results['workflowExecution']['workflowId'],
            workflow_run_id=results['workflowExecution']['runId'],
            workflow_type=results['workflowType'],
        )

    def walk_execution_history(
        self,
        workflow_id,
        workflow_run_id,
        reverse_order=True,
        use_raw_event_history=False,
        maximum_page_size=1000,
    ):
        """Lazily walks through the entire workflow history for a given workflow_id. This will make successive calls
        to SWF on demand when pagination is needed.

        See :meth:`~SWF.Client.get_workflow_execution_history` for more information.

        :param workflow_id: The workflow_id returned from :meth:`~py_swf.clients.decision.DecisionClient.poll`.
        :type identity: string
        :param workflow_run_id: The workflow_run_id returned from :meth:`~py_swf.clients.decision.DecisionClient.poll`.
        :type identity: string
        :param reverse_order: Passthru for reverseOrder to :meth:`~SWF.Client.get_workflow_execution_history`
        :type identity: bool
        :param use_raw_event_history: Whether to use the raw dictionary event history returned from AWS.
                                      Otherwise attempts to turn dictionaries into namedtuples recursively.
        :type use_raw_event_history: bool
        :param maximum_page_size: Passthru for maximumPageSize to :meth:`~SWF.Client.get_workflow_execution_history`
        :type identity: int

        :return: A generator that returns successive elements in the workflow execution history.
        :rtype: collections.Iterable
        """
        kwargs = dict(
            domain=self.decision_config.domain,
            reverseOrder=reverse_order,
            execution=dict(
                workflowId=workflow_id,
                runId=workflow_run_id,
            ),
            maximumPageSize=maximum_page_size,
        )

        while True:
            results = self.boto_client.get_workflow_execution_history(
                **kwargs
            )
            next_page_token = results.get('nextPageToken', None)
            events = results['events']

            for event in events:
                if not use_raw_event_history:
                    event = nametuplefy(event)
                yield event

            if next_page_token is None:
                break

            kwargs['nextPageToken'] = next_page_token

    def finish_decision_with_activity(self, task_token, activity_id, activity_name, activity_version, activity_input):
        """Responds to a given decision task's task_token to schedule an activity task to run.

        Passthrough to :meth:`~SWF.Client.respond_decision_task_completed`.

        :param task_token: The task_token returned from :meth:`~py_swf.clients.decision.DecisionClient.poll`.
        :type identity: string
        :param activity_id: A unique identifier for the activity task.
        :type identity: string
        :param activity_name: Which activity name to execute.
        :type identity: string
        :param activity_name: Version of the activity name.
        :type identity: string
        :param activity_input: Freeform text of the input for the activity
        :type identity: string
        :return: None
        :rtype: NoneType
        """
        activity_task = build_activity_task(
            activity_id,
            activity_name,
            activity_version,
            activity_input,
            self.decision_config,
        )

        self.boto_client.respond_decision_task_completed(
            taskToken=task_token,
            decisions=[activity_task],
        )

    def finish_workflow(self, task_token, result):
        """Responds to a given decision task's task_token to finish and terminate the workflow.

        Passthrough to :meth:`~SWF.Client.respond_decision_task_completed`.

        :param task_token: The task_token returned from :meth:`~py_swf.clients.decision.DecisionClient.poll`.
        :type identity: string
        :param result: Freeform text that represents the final result of the workflow.
        :type identity: string
        :return: None
        :rtype: NoneType
        """
        workflow_complete = build_workflow_complete(result)
        self.boto_client.respond_decision_task_completed(
            taskToken=task_token,
            decisions=[workflow_complete],
        )


def build_workflow_complete(result):
    return {
        'decisionType': 'CompleteWorkflowExecution',
        'completeWorkflowExecutionDecisionAttributes': {
            'result': result,
        },
    }


def build_activity_task(activity_id, activity_name, activity_version, input, decision_config):
    return {
        'decisionType': 'ScheduleActivityTask',
        'scheduleActivityTaskDecisionAttributes': {
            'activityType': {
                'name': activity_name,
                'version': activity_version,
            },
            'activityId': activity_id,
            'input': input,
            'taskList': {
                'name': decision_config.task_list,
            },
            'scheduleToCloseTimeout': str(decision_config.schedule_to_close_timeout),
            'scheduleToStartTimeout': str(decision_config.schedule_to_start_timeout),
            'startToCloseTimeout': str(decision_config.start_to_close_timeout),
            'heartbeatTimeout': str(decision_config.heartbeat_timeout),
        },
    }
