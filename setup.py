#!/usr/bin/env python

from setuptools import setup

setup(
name="pubcontrol",
version="2.0.5",
description="EPCP library",
author="Justin Karneges",
author_email="justin@affinix.com",
url="https://github.com/fanout/pypubcontrol",
license="MIT",
package_dir={'pubcontrol': 'src'},
packages=['pubcontrol'],
install_requires=["PyJWT>=0,<1"],
classifiers=[
	"Topic :: Utilities",
	"License :: OSI Approved :: MIT License"
]
)
