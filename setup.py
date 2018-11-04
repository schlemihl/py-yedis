#!/usr/bin/env python
import os

from yedis import __version__


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


f = open(os.path.join(os.path.dirname(__file__), 'README.rst'))
long_description = f.read()
f.close()

setup(
    name='yedis',
    version=__version__,
    description='Python client to the Yedis API of YugaByteDB.',
    long_description=long_description,
    keywords=[
        'Redis', 'key-value store',
        'Yedis', 'YugaByteDB', 'time series',
    ],
    license='MIT',
    packages=['yedis'],
    require=[
        'redis'
    ],
    tests_require=[
    ],
)
