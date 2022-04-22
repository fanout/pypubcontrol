#!/usr/bin/env python

from setuptools import setup

setup(
	name='pubcontrol',
	version='3.3.0',
	description='EPCP library',
	author='Justin Karneges',
	author_email='justin@fanout.io',
	url='https://github.com/fanout/pypubcontrol',
	license='MIT',
	package_dir={'pubcontrol': 'src'},
	packages=['pubcontrol'],
	install_requires=[
		'PyJWT>=1,<2;python_version<"3.0"',
		'PyJWT>=1,<3;python_version>="3.0"',
		'requests>=2.4,<3'
	],
	classifiers=[
		'Topic :: Utilities',
		'License :: OSI Approved :: MIT License'
	]
)
