# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class NoTaskFound(Exception):
    """Raised when polling for activity or decision tasks times out.
    """
    pass
