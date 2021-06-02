#!/usr/bin/env python

from setuptools import setup
import sys

if sys.version_info >= (3,):
	install_requires = ['PyJWT>=1,<3', 'requests>=2.4,<3']
else:
	install_requires = ['PyJWT>=1,<2', 'requests>=2.4,<3']

setup(
	name='pubcontrol',
	version='3.2.0',
	description='EPCP library',
	author='Justin Karneges',
	author_email='justin@fanout.io',
	url='https://github.com/fanout/pypubcontrol',
	license='MIT',
	package_dir={'pubcontrol': 'src'},
	packages=['pubcontrol'],
	install_requires=install_requires,
	classifiers=[
		'Topic :: Utilities',
		'License :: OSI Approved :: MIT License'
	]
)
