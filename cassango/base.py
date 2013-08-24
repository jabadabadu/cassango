#-*- coding: iso-8859-2 -*-
import pycassa.ConsistencyLevel as CL

from pycassa.system_manager import *
from pycassa.pool import ConnectionPool as Connect
from django.db import DEFAULT_DB_ALIAS
from django.db.backends import BaseDatabaseWrapper, BaseDatabaseFeatures, BaseDatabaseOperations
from django.core.exceptions import ImproperlyConfigured
from .client import DatabaseClient

class DatabaseFeatures(BaseDatabaseFeatures):
    can_return_id_from_insert = True
    supports_joins = False
    supports_select_related = False
    supports_deleting_related_objects = False
    interprets_empty_strings_as_nulls = True
    can_combine_inserts_with_and_without_auto_increment_pk = True
    supports_timezones = False
    supports_transactions = None
    supports_stddev = None

    def _supports_transactions(self): 
    	return False

    def _supports_stddev(self):
        return False

class DatabaseOperations(BaseDatabaseOperations):
	pass

class DatabaseValidation(BaseDatabaseValidation):
	def __init__(self, connection):
		self.connection = connection

	def validate_field(self, errors, opts, f):
		pass

class DatabaseIntrospection(BaseDatabaseIntrospection):
	pass
                                                                                                                                                          
class DatabaseWrapper(BaseDatabaseWrapper):
	vendor = 'cassandra'

	operators = {
        		 'exact'		:	'= %s',
        		 'iexact'		:	'= UPPER(%s)',
        		 'contains'		:	'LIKE %s',
        		 'icontains'	:	'LIKE UPPER(%s)',
        		 'regex'		:	'~ %s',
        		 'iregex'		:	'~* %s',
        		 'gt'			:	'> %s',
        		 'gte'			:	'>= %s',
        		 'lt'			:	'< %s',
        		 'lte'			:	'<= %s',
        		 'startswith'	:	'LIKE %s',
        		 'endswith'		:	'LIKE %s',
        		 'istartswith'	:	'LIKE UPPER(%s)',
        		 'iendswith'	:	'LIKE UPPER(%s)',
        		 }


	def __init__(self, *args, **kwargs):
		super(DatabaseWrapper, self).__init__(*args, **kwargs)
		self.connection = None
		self.manager = None
		self.connected = False
		self.settings_dict = settings_dict
		self.alias = 'default'

		self.features = DatabaseFeatures(self)
		self.ops = DatabaseOperations(self)
		self.creation = DatabaseCreation(self)
		self.validation = DatabaseValidation(self)
		self.introspection = DatabaseIntrospection(self)

		try:
			self.read_consistency = self.settings_dict.get('CASSANDRA_READ_CONSISTENCY_LEVEL', CL.ONE)
			self.write_consistency = self.settings_dict.get('CASSANDRA_WRITE_CONSISTENCY_LEVEL', CL.ONE)
			self.topology = self.settings_dict.get('CASSANDRA_TOPOLOGY', SIMPLE_STRATEGY)
		except Exception, e:
			raise ImproperlyConfigured("You need to specify Cassandra main settings such as: read/write consistency level, topology.")


	def connect(self):
		settings_dict = self.settings_dict

		if self.connection is None and self.connected is False:
			if settings_dict['NAME'] == '':
				raise ImproperlyConfigured("You need to specify NAME in Django settings file.")

			connection_parameters = {
				'keyspace': settings_dict['NAME'],
			}
			
			connection_parameters_credentials = {}

			if settings_dict['USER']:
				connection_parameters_credentials['username'] = settings_dict['USER']
			if settings_dict['PASSWORD']:
				connection_parameters_credentials['password'] = settings_dict['PASSWORD']
			if settings_dict['HOST'] and settings_dict['PORT']:
				connection_parameters['server_list'] = str(settings_dict['HOST']) + ':' + str(settings_dict['PORT'])
			elif settings_dict['HOST'] == '':
				connection_parameters['server_list'] = 'localhost'
			else:
				raise ImproperlyConfigured("You need to specify HOST and PORT in Django settings file.")

			if len(connection_parameters_credentials) == 0:
				self.connection = Connect(
										   keyspace=connection_parameters['keyspace'], 
										   server_list=connection_parameters['server_list']
										   )
				self.manager = SystemManager(
											 server=connection_parameters['server_list']
											 )
			else:
				self.connection = Connect(
										   keyspace=connection_parameters['keyspace'], 
										   server_list=connection_parameters['server_list'], 
										   credentials=connection_parameters_credentials
										   )

				self.manager = SystemManager(
											 server=connection_parameters['server_list'], 
											 credentials=connection_parameters_credentials
											 )

			self.connected = True

		return self.connection


	def reconnect(self):
		if self.connected is True and self.connection is not None:
			self.connection.dispose()
			self.manager.close()
			self.connected = False
			self.connection = None
		self.connect()


	def close(self):
		if self.connected is True and self.connection is not None:
			self.connection.dispose()
			self.manager.close()
			self.connected = False
			self.connection = None
