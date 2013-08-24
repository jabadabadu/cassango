#-*- coding: iso-8859-2 -*-

import os
import sys

from django.db.backends import BaseDatabaseClient

class DatabaseClient(BaseDatabaseClient):
	executable_name = 'pycassaShell'

	def __init__(self, connection):
		self.connection = connection

	def runshell(self):
		settings_dict = self.connection.settings_dict

		args = [self.executable_name]

		if settings_dict['NAME']:
			args.extend(["-k", settings_dict['NAME']])
		
		if settings_dict['USER']:
			args.extend(["-u", settings_dict['USER']])

		if settings_dict['PASSWORD']:
			args.extend(["-P", settings_dict['PASSWORD']])

		if settings_dict['HOST']:
			args.extend(["-H", settings_dict['HOST']])

		if settings_dict['PORT']:
			args.extend(["-p", str(settings_dict['PORT'])])
