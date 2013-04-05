#!/usr/bin/env python

from setuptools import setup

setup(
name="pubcontrol",
version="1.0.2",
description="EPCP library",
author="Justin Karneges",
author_email="justin@affinix.com",
url="https://github.com/fanout/pypubcontrol",
license="MIT",
py_modules=["pubcontrol"],
install_requires=["PyJWT>=0.1.5", "pyzmq>=2.0,<3.0"],
classifiers=[
	"Topic :: Utilities",
	"License :: OSI Approved :: MIT License"
]
)
