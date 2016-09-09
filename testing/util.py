# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import defaultdict

from mock import Mock


class DictMock(Mock):
    """Like Mock, except also "virally" returns DictMocks
    upon __getitem__
    """
    __dict_contents = defaultdict(lambda: DictMock())

    def __getitem__(self, name):
        return self.__dict_contents[name]

    def __setitem__(self, name, val):
        self.__dict_contents[name] = val
