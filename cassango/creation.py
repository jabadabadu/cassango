#-*- coding: iso-8859-2 -*-

import sys
import time


from django.conf import settings
from django.core.management import call_command
from django.db.utils import load_backend
from pycassa.types import *


TEST_DATABASE_PREFIX = 'test_'


class CassandraDatabaseCreation(object):
    data_types = {
                  'AutoField'                   :   'text',
                  'BooleanField'                :   'boolean',
                  'CharField'                   :   'text',
                  'CommaSeparatedIntegerField'  :   'text',
                  'DateField'                   :   'date',
                  'DateTimeField'               :   'datetime',
                  'DecimalField'                :   'decimal',
                  'EmailField'                  :   'text',
                  'FileField'                   :   'text',
                  'FilePathField'               :   'text',
                  'FloatField'                  :   'float',
                  'IntegerField'                :   'integer',
                  'BigIntegerField'             :   'integer',
                  'IPAddressField'              :   'text',
                  'GenericIPAddressField'       :   'text',
                  'NullBooleanField'            :   'boolean',
                  'OneToOneField'               :   'integer',
                  'PositiveIntegerField'        :   'integer',
                  'PositiveSmallIntegerField'   :   'integer',
                  'SlugField'                   :   'text',
                  'SmallIntegerField'           :   'integer',
                  'TextField'                   :   'text',
                  'TimeField'                   :   'time',
                  }

    
    def __init__(self, manager):
        self.manager = manager


    def sql_create_model(self, model, style, known_models=set()):
        keyspace_name = self.connection.settings_dict['NAME']

        opts = model._meta

        if not opts.managed or opts.proxy or opts.swapped:
            return [], {}

        column_validators = {}

        for f in opts.local_fields:
            col_name = str(f.column)
            col_type = f.db_type(connection=self.connection)

            if col_type in ['CharField', 'CommaSeparatedIntegerField', 'EmailField', 'FileField', 'FilePathField',
                            'IPAddressField', 'GenericIPAddressField', 'SlugField', 'TextField']:
                col_type = UTF8Type
            if col_type in ['IntegerField', 'OneToOneField', 'PositiveIntegerField', 
                            'PositiveSmallIntegerField', 'SmallIntegerField']:
                col_type = IntegerType
            if col_type in ['BooleanField', 'NullBooleanField']:
                col_type = AsciiType
            if col_type == 'DecimalField':
                col_type = DecimalType
            if col_type == 'DateTimeField':
                col_type = DateType
            if col_type == 'FloatField':
                col_type = FloatType
            if col_type == 'BigIntegerField':
                col_type = LongType

            column_validators[col_name] = data_types[col_type]

        column_family_name = opts.db_table

        if not self.connection.settings_dict['comparator_type']:
            comparator_type = 'UTF8Type'

        self.manager.create_column_family(keyspace=keyspace_name, 
                                          name=column_family_name,
                                          comparator_type=comparator_type, 
                                          column_validation_classes=column_validators)

        return [], {}


    def create_test_db(self, verbosity=1, autoclobber=False):
        test_database_name = self.get_test_db_name()

        self.connection.reconnect()
        self.drop_db(test_database_name)
        call_command('syncdb', 
                     verbosity=max(verbosity-1, 0), 
                     interactive=False,
                     database=self.connection.alias
                     )

        return test_database_name


    def destroy_test_db(self, old_database_name, verbosity=1):
        if verbosity >= 1:
            print "Destroying test database for alias '%s'..." % self.connection.alias

        test_database_name = self.connection.settings_dict['NAME']
        self.drop_db()


    def drop_db(self, database_name, verbosity):
        self.manager.drop_keyspace(database_name)


    def delete_test_cassandra_keyspace(self, keyspace_name):
        settings_dict = self.connection.settings_dict
        test_keyspace_name = settings_dict.get('NAME')

        self.drop_cassandra_keyspace(keyspace_name)
        self.connection.settings_dict['NAME'] = old_database_name

    def get_test_db_name(self):
        settings_dict = self.connection.settings_dict

        if settings_dict.has_key('TEST_NAME'):
            test_keyspace_name = settings_dict['TEST_NAME']
        else:
            test_keyspace_name = TEST_DATABASE_PREFIX + settings_dict['NAME']

        return test_keyspace_name

    def test_db_signature(self):
        settings_dict = self.connection.settings_dict

        return (
            settings_dict['HOST'],
            settings_dict['PORT'],
            settings_dict['ENGINE'],
            settings_dict['NAME']
        )