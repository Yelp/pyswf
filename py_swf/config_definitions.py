# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple


WorkflowClientConfig = namedtuple(
    'WorkflowClientConfig',
    'domain task_list execution_start_to_close_timeout task_start_to_close_timeout',
)
"""An immutable object that stores common SWF values. Used by instances of :class:`~py_swf.clients.workflow.WorkflowClient`.
"""

StartWorkflowResult = namedtuple(
    'StartWorkflowResult',
    'run_id'
)
"""
An immutable object that stores response after calling start_workflow_execution.
Used by instances of :class:`~py_swf.clients.workflow.WorkflowClient`.
"""

CountWorkflowsResult = namedtuple(
    'CountWorkflowsResult',
    'count truncated',
)
"""An immutable object that stores results of counting workflows. Used by instances of :class:`~py_swf.clients.WorkflowClient`.
"""

DecisionConfig = namedtuple(
    'DecisionConfig',
    'domain task_list schedule_to_close_timeout schedule_to_start_timeout start_to_close_timeout heartbeat_timeout',
)
"""An immutable object that stores common SWF values. Used by instances of :class:`~py_swf.clients.decision.DecisionClient`.
"""

ActivityTaskConfig = namedtuple(
    'ActivityTaskConfig',
    'domain task_list',
)
"""An immutable object that stores common SWF values. Used by instances of :class:`~py_swf.clients.ActivityTaskClient`.
"""
