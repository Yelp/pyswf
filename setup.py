# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import find_packages
from setuptools import setup

import pyswf

readme = open('README.rst').read()
doclink = """
Documentation
-------------

The full documentation is at https://github.com/Yelp/pyswf/"""
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    # py2 + setuptools asserts isinstance(name, str) so this needs str()
    name=str('pyswf'),
    version=pyswf.__version__,
    description="A SWF client library that makes things easy for building workflow logic",
    long_description=readme + '\n\n' + doclink + '\n\n' + history,
    author=pyswf.__author__,
    author_email=pyswf.__email__,
    url='https://github.com/Yelp/pyswf/',  # TODO: add readthedocs.io documentation
    packages=find_packages(exclude=['tests*']),
    install_requires=[
        'boto3==1.2.1',
        'botocore==1.3.7',
    ],
    zip_safe=False,
    keywords='pyswf',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
