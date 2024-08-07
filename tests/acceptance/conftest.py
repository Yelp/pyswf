# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import random

import boto3
import pytest
import yaml
from botocore.client import Config
from staticconf.loader import DictConfiguration

from py_swf.clients.activity_task import ActivityTaskClient
from py_swf.clients.admin import WorkflowRegistrar
from py_swf.clients.decision import DecisionClient
from py_swf.clients.workflow import WorkflowClient
from py_swf.config_definitions import ActivityTaskConfig
from py_swf.config_definitions import DecisionConfig
from py_swf.config_definitions import WorkflowClientConfig


@pytest.fixture
def boto_client():
    botocore_config = Config(
        connect_timeout=5,
        read_timeout=1,
    )
    return boto3.Session().client('swf', config=botocore_config)


@pytest.fixture
def domain_name():
    return 'test_domain'


@pytest.fixture
def task_list():
    return str(random.getrandbits(32))


@pytest.fixture
def version():
    return '0.1'


@pytest.fixture
def workflow_name():
    return 'test_workflow'


@pytest.fixture
def activity_name():
    return 'test_activity'


@pytest.fixture
def workflow_registrar(boto_client, domain_name):
    registrar = WorkflowRegistrar(boto_client)
    registrar.register_domain(
        name=domain_name,
    )
    return registrar


@pytest.fixture
def decision_client(boto_client, domain_name, task_list):
    decision_config = DecisionConfig(
        domain=domain_name,
        task_list=task_list,
        schedule_to_close_timeout=5,
        schedule_to_start_timeout=5,
        start_to_close_timeout=5,
        heartbeat_timeout=5,
    )
    return DecisionClient(decision_config, boto_client)


@pytest.fixture
def workflow_client(boto_client, domain_name, task_list, workflow_name, version, workflow_registrar):
    workflow_registrar.register_workflow_type(
        domain=domain_name,
        name=workflow_name,
        version=version,
    )
    workflow_client_config = WorkflowClientConfig(
        domain=domain_name,
        task_list=task_list,
        execution_start_to_close_timeout=5,
        task_start_to_close_timeout=5,
    )
    return WorkflowClient(workflow_client_config, boto_client)


@pytest.fixture
def activity_task_client(boto_client, workflow_registrar, domain_name, activity_name, version, task_list):
    workflow_registrar.register_activity_type(
        domain=domain_name,
        name=activity_name,
        version=version,
    )
    task_config = ActivityTaskConfig(
        domain=domain_name,
        task_list=task_list,
    )
    return ActivityTaskClient(task_config, boto_client)
