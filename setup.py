#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-pardot",
    version="1.5.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_pardot"],
    install_requires=[
        "singer-python==6.8.0",
        "requests==2.33.0",
        "backoff==2.2.1",
        "python-dateutil==2.9.0",
    ],
    extras_require={
        'dev': [
            'pylint',
            'pytest',
            'coverage',
            'parameterized',
        ],
    },
    entry_points="""
    [console_scripts]
    tap-pardot=tap_pardot:main
    """,
    packages=["tap_pardot"],
    package_data={"schemas": ["tap_pardot/schemas/*.json"]},
    include_package_data=True,
)
