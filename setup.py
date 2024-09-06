#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Setup module """
import os

from setuptools import setup


def get_install_requirements(path):
    content = open(os.path.join(os.path.dirname(__file__), path)).read()
    return [req for req in content.split("\n") if req != "" and not req.startswith("#")]


VERSION = "0.29.0.1"

here = os.path.dirname(__file__)

# Get long description
README = open(os.path.join(os.path.dirname(__file__), "README.rst")).read()

setuptools_kwargs = {
    "install_requires": ["requests", "six", "httpx[http2]"],
    "zip_safe": False,
}

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name="ksql-python-ng",
    version=VERSION,
    description="A Python wrapper for the KSQL REST API",
    long_description=README,
    author="Bryan Yang",
    author_email="kenshin200528@gmail.com",
    maintainer="Idan Sheinberg",
    maintainer_email="ishinberg0@gmail.com",
    url="https://github.com/sheinbergon/ksql-python-ng",
    license="MIT License",
    packages=["ksql"],
    include_package_data=True,
    platforms=["any"],
    extras_require={"dev": get_install_requirements("requirements_test.txt")},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    **setuptools_kwargs,
)
