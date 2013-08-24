#-*- coding: iso-8859-2 -*-

import datetime
import sys
import traceback
import decimal


from django.db.models.fields import NOT_PROVIDED
from django.db.models.sql.constants import LOOKUP_SEP, MULTI
from django.db.models.sql.where import AND, OR, WhereNode
from django.db.utils import DatabaseError, IntegrityError
from django.utils.encoding import smart_str


from djangotoolbox.db.basecompiler import NonrelQuery, NonrelCompiler, NonrelInsertCompiler, NonrelUpdateCompiler, NonrelDeleteCompiler
from functools import wraps
#from pycassa.batch import Mutator
from pycassa.columnfamily import ColumnFamily as CF
from uuid import uuid4


def convert_list_to_string(l):
    """
    Converts list to string

    :param l: Input list
    """

    lts = " ".join(str(s) for s in l)

    return lts


def convert_string_to_list(s):
    """
    Converts string to list

    :param s: Input string
    """

    stl = s.split()

    return stl


def safe_call(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DatabaseError, e:
            raise DatabaseError, DatabaseError(str(e)), sys.exc_info()[2]
    return wrapper


def get_pk_column(self):
    """
    Gets the primary key column.
    """

    return self.query.get_meta().pk.column


def get_column_family(self):
    """
    Gets the name of the column family.
    """
    
    return self.query.get_meta().db_table


def CassandraQuery(object):
    """
    Base class for cassandra queries.

    Compilers build a nonrel query when they want to fetch data. This class 
    provides filtering and ordering. It also gives you the opportunity to 
    convert SQL constraint tree built by Django framework to a more suitable
    representation for a Cassandra database. 
    """


    def __init__(self, compiler, fields):
        super(CassandraQuery, self).__init__(compiler, fields)

        self.column_family = get_column_family()
        self.root_predicate = None
        self.cached_results = None
        self.ordering = []
        self.cassandra_query = getattr(compiler.query, 'raw_query', {})

        self.indexed_cols = []
        self.field_to_col_name = {}

        for field in fields:
            if field.db_column:
                column_name = field.db_column
            elif field.column:
                column_name = field.column

            if field.db_index:
                self.indexed_cols.append(column_name)
            self.field_to_col_name[field.name] = column_name


    def __repr__(self):
        return '<CassandraQuery: %r ORDER %r>' % (self.cassandra_query, self.ordering)


    @safe_call
    def fetch(self, low_mark, high_mark):
        if self.root_predicate == None:
            raise DatabaseError("No root query node.")

        try:
            if high_mark is not None and high_mark <= low_mark:
                yield []

            results = self.get_results()

            if low_mark is not None or high_mark is not None:
                results = results[low_mark:high_mark]
        except Exception, e:
            raise DatabaseError(str(e)), sys.exc_info()[1], e

        for entity in results:
            yield entity
            return

    @safe_call
    def count(self, limit=None):
        """
        Returns the number of objects that would be returned, if
        this query was executed, up to `limit`.

        :param limit: The maximum number of results to return
        """
        results = self.get_results()

        res = []
        if limit is not None:
            for l in range(limit):
                res.append(results[l])

            results = res

        return len(results)


    @safe_call
    def order_by(self, ordering):
        """
        Reorders query results or execution order. Called by
        NonrelCompilers during query building.

        :param ordering: A list with (field, ascending) tuples or a
                         boolean -- use natural ordering, if any, when
                         the argument is True and its reverse otherwise
        """

        for order in ordering:
            if LOOKUP_SEP in order:
                raise DatabaseError("Ordering cannot span tables on"
                                    "non-relational backends (%s)." % order)
            if order == "?":
                raise DatabaseError("Randomized ordering are not supported!")

            if not order.startswith('-'):
                reversed = False
            else:
                order = order[1:]
                reversed = True
                
            column_name = self.field_to_col_name.get(order, order)

            self.ordering.append((column_name, reversed))


    @safe_call
    def delete(self):
	pool = self.connection
        column_family_name = get_column_family()
        col_fam = CF(pool, column_family_name)

        results = self.get_results()
        

    @safe_call
    def get_results(self):
        """
        ADD BODY!!!!!!!
        """     

class SQLCompiler():
    """
    Base class for all Cassandra compilers
    """
    query_class = CassandraQuery

    def get_column_family(self):
        """
        Function for getting name of the column family.
        """
        return self.query.get_meta().db_table

    def split_database_type(self, db_type):
        try:
            db_type, db_subtype = db_type.split(':', 1)
        except ValueError:
            db_subtype = None
        return db_type, db_subtype

    def make_result(self, entity, fields):
        result = []

        for field in fields:
            value = entity.get(field.column, NOT_PROVIDED)
            if value is NOT_PROVIDED:
                value = field.get_default()
            else:
                value = self.convert_value_from_db(
                    field.db_type(
                        connection = self.connection
                    ), value)

            if not field.null and value is None:
                raise DatabaseError("Non-nullable field %s can't be None!" % field.name)

            result.append(value)
        return result

    def convert_value_from_db(self, db_type, value):
        if db_type is None:
            return value

        if value is None or value is NOT_PROVIDED:
            return None

        db_type, db_subtype = self.split_database_type(db_type)

        if db_type.startswith('ListField') and db_subtype is not None:
            value = convert_string_to_list(value)
            if isinstance(value, (set, list, tuple)):
                value = [self.convert_value_from_db(db_subtype, subvalue) for subvalue in value]
            # elif isinstance(value, dict):
            #     value = dict((key, self.convert_value_from_db(db_subtype, subvalue) for key, subvalue in value.iteritems()))
        elif db_type == 'date':
            value = datetime.date(value.year, value.month, value.day)
            #dt = datetime.datetime.strptime(value, '%Y-%m-%d')
            #value = dt.date()
        elif db_type == 'datetime':
            value = datetime.datetime(value.year, value.month, value.day, value.hour, value.minute, value.second, value.microsecond)
            #value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        elif db_type == 'time':
            value = datetime.time(value.hour, value.minute, value.second, value.microsecond)
            #dt = datetime.datetime.strptime(value, '%H:%M:%S.%f')
            #value = dt.time()
        elif db_type == 'bool':
            value = value.lower() == 'true'
        elif db_type == 'int':
            value = int(value)
        elif db_type == 'long':
            value = long(value)
        elif db_type == 'float':
            value = float(value)

        return value

    def convert_value_for_db(self, db_type, value):
        if db_type is None or value is None:
            return value

        db_type, db_subtype = self.split_database_type(db_type)

        if db_type.startswith('ListField'):
            if db_subtype is not None:
                if isinstance(value, (set, list, tuple)):
                    value = [self.convert_value_for_db(db_subtype, subvalue) for subvalue in value]
                    value = convert_list_to_string(value)
                elif isinstance(value, dict):
                    value = dict((key, self.convert_value_for_db(db_subtype, subvalue)) for key, subvalue in value.iteritems())
        elif isinstance(value, (set, list, tuple)):
            value = [self.convert_value_for_db(db_type, val) for val in value]
        elif db_type == 'date':
            value = value.strftime('%Y-%m-%d')
        elif db_type == 'datetime':
            value = value.strftime('%Y-%m-%d %H:%M:%S.%f')
        elif db_type == 'time':
            value = value.strftime('%H:%M:%S.%f')
        elif db_type == 'bool':
            value = str(value).lower()
        elif db_type == 'int':
            value = str(value)
        elif db_type == 'long':
            value = str(value)
        elif db_type == 'float':
            value = str(value)
        elif (type(value) is not str) and (type(value) is not unicode):
            value = unicode(value)

        if type(value) is unicode:
                value = value.encode('utf-8')

        return value

class SQLInsertCompiler(NonrelInsertCompiler, SQLCompiler):

    @safe_call
    def insert(self, data, return_id = False):
        """
        Creates a new entity to represent a model.

        :param data: Model object represented by a list of (field, value) pairs.
                     Each value is prepared for the insert operation.
        :param return_id: Value whether to return the id or key of newly created
                          entity.
        """

        pool = self.connection
        column_family_name = get_column_family()
        col_fam = CF(pool, column_family_name)

        col_fam_data = {}
        for field, value in data.iteritems():
            col_fam_data[field] = value

        key = data.get(pk_column)
        
        if not key:
            key = str(uuid4())
            
        try:
            col_fam.insert(key=key,
                           columns=col_fam_data,
                           write_consistency_level=self.connection.write_consistency_level)
        except Exception, e:
            print str(e)
    
        if return_id:
            return key

class SQLUpdateCompiler(NonrelUpdateCompiler, SQLCompiler):
    query_class = CassandraQuery

    def execute_sql(self, result_type):
        data = {}

        for field, model, value in self.query.values:
            assert field is not None

            if value is None and not field.null:
                raise DatabaseError("It is not possible to set None to a ",
                                    "non-nullable field %s!" % field.name)

            db_type = field.db_type(connection=self.connection)
            value = self.convert_value_for_db(db_type, value)

            data[field.column] = value

        return self.update(data)

    def update(self, values):
        """
        Changes an entity that already exists in the database.

        :param values: A list of (field, new-value) pairs.
        """

        pool = self.connection
        column_family_name = get_column_family()
        col_fam = CF(pool, column_family_name)

        pk_column = get_pk_column()
    
        pk_index = -1
        fields = self.get_fields()

        for index in range(len(fields)):
            if fields[index].column == pk_column:
                pk_index = index
                break
        
        if pk_index == -1:
            raise DatabaseError('Invalid primary key column.')

        b = col_fam.batch(write_consistency_level=self.connection.write_consistency_level)
        row_count = 0
        for result in self.results_iter():
            row_count += 1
            key = result[pk_index]

            for k, v in values.items():
                b.insert(k, v)

        b.send()

        return row_count
