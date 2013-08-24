from djangotoolbox.db.base import NonrelDatabaseIntrospection


class DatabaseIntrospection(NonrelDatabaseIntrospection):
	# def __init__(self, manager):
	# 	self.manager = manager


	def get_cass_keyspace_list(self):
		return self.manager.list_keyspaces()


	def get_cass_keyspace_properties(self, keyspace_name):
		return self.manager.get_keyspace_properties(keyspace_name)


	def get_cass_column_families(self):
		return self.connection.get().get_keyspace_description().keys()


	def get_cass_keyspace_column_families(self, keyspace_name):
		return self.manager.get_keyspace_column_families(keyspace_name)