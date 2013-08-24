#!/usr/bin/env python
# -*- coding: iso-8859-2 -*-
from ez_setup import use_setuptools
use_setuptools()

import codecs
import os
import sys
import cassango as cngo
from setuptools import setup, find_packages

readme_file = os.path.join(os.path.dirname(__file__), 'README.rst')

try:
	long_description = open(readme_file).read()
except IOError, err:
	sys.stderr.write("[ERROR] Cannot find file specified as ""``long_description`` (%s)\n" % readme_file)
	sys.exit(1)

setup(
	name = 'cassango',
	version='.'.join(map(str, cngo.__version__)),
	author = cngo.__author__,
	author_email = cngo.__contact__,
	url = 'http://github.com/jabadabadu/django_cassandra',
	download_url = 'http://github.com/jabadabadu/django_cassandra/downloads',
	description= 'Django Cassandra Engine for django',
	long_description = long_description,
	packages = find_packages('cassango'),
	package_dir = {'':'cassango'},
    package_data = {'':['*.py']},
	include_package_data = True,
	scripts = [],
	requires = [],
	license = 'BSD License',
	install_requires = [
		'Django >= 1.3',
		'pycassa >= 1.8.0',
	],
	classifiers = [
		'Development Status :: 2 - Pre-Alpha',
		'Environment :: Web Environment',
		'Framework :: Django',
		'Intended Audience :: Developers',
		'License :: OSI Approved :: BSD License',
		'Operating System :: OS Independent',
		'Programming Language :: Python',
		'Programming Language :: Python :: 2.6',
		'Programming Language :: Python :: 2.7',
		'Topic :: Database',
        	'Topic :: Database :: Database Engines/Servers',
		'Topic :: Internet',
		'Topic :: Software Development :: Libraries :: Python Modules',
        	'Topic :: Utilities',
	],
	keywords = 'django, cassandra, orm, nosql, database, python',
)

