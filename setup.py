# coding: utf-8

from os.path import dirname, abspath, exists

from setuptools import setup, find_packages  # noqa: H301

NAME = "synmax-api-python-client"

# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = ["urllib3", "requests", ]
# DEV_REQUIRES = ["pytest", "black"]

DIR_PATH = dirname(abspath(__file__))

with open(DIR_PATH + "/VERSION", "r", encoding="utf-8") as f:
    VERSION = f.readline().strip()

LONG_DESCRIPTION = None
if exists(DIR_PATH + "/README.md"):
    with open(DIR_PATH + "/README.md", "r", encoding="utf-8") as f:
        LONG_DESCRIPTION = f.read()

setup(
    name=NAME,
    packages=find_packages(),
    version=VERSION,
    description="Synmax API client",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/SynMaxDev/synmax-api-python-client.git",
    author="Eric Anderson and Deepa Aswathaiah",
    author_email="",
    install_requires=REQUIRES,
    # extras_require={"dev": DEV_REQUIRES},
    python_requires=">=3.7",
    include_package_data=True,
    package_data={
        'DATA': ['fips_lookup.csv'],
    },
)
