#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-pardot",
    version="1.4.8",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_pardot"],
    install_requires=["singer-python==5.13.2", "requests==2.32.4", "backoff==1.10.0"],
    entry_points="""
    [console_scripts]
    tap-pardot=tap_pardot:main
    """,
    packages=["tap_pardot"],
    package_data={"schemas": ["tap_pardot/schemas/*.json"]},
    include_package_data=True,
)
