# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest


@pytest.fixture
def boto_client():
    return mock.Mock()
