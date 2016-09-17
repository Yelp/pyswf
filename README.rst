=======================================================
py_swf
=======================================================

.. image:: https://travis-ci.org/Yelp/pyswf.svg?branch=master
    :target: https://travis-ci.org/Yelp/pyswf

.. image:: https://coveralls.io/repos/github/Yelp/pyswf/badge.svg?branch=master
    :target: https://coveralls.io/github/Yelp/pyswf?branch=master

.. image:: https://readthedocs.org/projects/py-swf/badge/?version=latest
    :target: http://py-swf.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://img.shields.io/pypi/v/py-swf.svg
    :target: https://pypi.python.org/pypi/py-swf/

py_swf is a library that provides a pythonic way to interact with the boto3 SWF client. It provides a thin client above boto3 and tries to provide the same API as boto3's SWF client. This library tries to group the API calls into SWF's framework of deciders, activity runners, and a client that can initiate and terminate workflows. More information on inputs to boto3 can be found in the boto3 :class:`~SWF.Client` class.

The library provides 4 clients: 
 - A client that allows starting and force-termination of workflows.
 - A client that can poll for decision tasks and respond to decision tasks.
 - A client that can poll for activity tasks and respond to activity tasks.
 - A client that can perform registration of workflows in SWF.

Consumers of this library are expected to write their own daemons that enact business logic.

Example daemon that listens on decision tasks

.. code-block:: python

        import boto3
        from py_swf.config_definitions import DecisionConfig
        from py_swf.clients.decision import DecisionClient

        boto_client =  boto3.Session(...).client('swf')
        decision_config = DecisionConfig(...)

        client = DecisionClient(decision_config, boto_client)

        while True:
                task = client.poll()

                ... = perform_decision_task(task)

                client.finish_decision_with_activity(
                        task.task_token,
                        ...
                )

The heart of the daemon is the :class:`~py_swf.clients.decision.DecisionClient`. You must provide a valid bare boto3 client, and :class:`~py_swf.config_definitions.DecisionConfig` which represents common SWF inputs, such as domain, and some timeouts. 

Likewise, you must implement an activity runner:

.. code-block:: python
        
        import boto3
        from py_swf.config_definitions import ActivityTaskConfig
        from py_swf.clients.activity_task import ActivityTaskClient

        boto_client =  boto3.Session(...).client('swf')
        activity_task_config = ActivityTaskConfig(...)

        client = ActivityTaskClient(decision_config, boto_client)

        while True:
                task = client.poll()

                result = perform_activity_task(task.input)

                client.finish(
                        task_token=task.task_token,
                        result=result,
                )

Activity task runners are very similar to deciders. However, they don't have any information of the workflow, and only perform one task given an input and responds with an output.
