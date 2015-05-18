#!/usr/bin/env python

from setuptools import setup
import sys

if sys.version_info >= (3,):
	install_requires = ['PyJWT>=0,<1','requests>=2,<3','tnetstring3>=0,<1']
else:
	install_requires = ['PyJWT>=0,<1','requests>=2,<3','tnetstring>=0,<1']

setup(
	name='pubcontrol',
	version='2.2.2',
	description='EPCP library',
	author='Justin Karneges',
	author_email='justin@affinix.com',
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
