# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import find_packages
from setuptools import setup

import py_swf

readme = open('README.rst').read()
doclink = """
Documentation
-------------

The full documentation is at http://py-swf.readthedocs.io/en/latest/"""
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    # py2 + setuptools asserts isinstance(name, str) so this needs str()
    name=str('py-swf'),
    version=py_swf.__version__,
    description="A SWF client library that makes things easy for building workflow logic",
    long_description=readme + '\n\n' + doclink + '\n\n' + history,
    author=py_swf.__author__,
    author_email=py_swf.__email__,
    url='http://py-swf.readthedocs.io/en/latest/',
    packages=find_packages(exclude=['tests*', 'testing']),
    install_requires=[
        'boto3',
        'botocore>=1.3.24',
    ],
    zip_safe=False,
    keywords=['py_swf', 'swf', 'amazon', 'workflow'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
