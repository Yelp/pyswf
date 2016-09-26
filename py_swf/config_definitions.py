# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple


WorkflowClientConfig = namedtuple(
    'WorkflowClientConfig',
    'domain workflow_name workflow_version task_list execution_start_to_close_timeout task_start_to_close_timeout',
)
"""An immutable object that stores common SWF values. Used by instances of :class:`~py_swf.clients.workflow.WorkflowClient`.
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
