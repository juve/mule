# Copyright 2010 University Of Southern California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import sys
import os
import sqlite3
import cPickle as pickle
from mule import log, config

def with_connection(method):
	def with_connection(self, *args, **kwargs):
		if len(args)>0 and isinstance(args[0], sqlite3.Connection):
			return method(self, *args, **kwargs)
		else:
			conn = self.get_connection()
			try:
				result = method(self, conn, *args, **kwargs)
				conn.commit()
				return result
			finally:
				conn.close()
	return with_connection

class Row(object):
	"""
	This class is here because Python 2.5 doesn't have a
	row implementation that acts like a dictionary.
	"""
	def __init__(self, cursor, row):
		self.row = row
		self.cols = {}
		i = 0
		for col in cursor.description:
			self.cols[col[0]] = i
			i += 1
			
	def __getitem__(self, i):
		if isinstance(i, str):
			return self.row[self.cols[i]]
		else:
			return self.row[i]
	
	def __iter__(self):
		x = [(k, self.row[self.cols[k]]) for k in self.cols]
		return x.__iter__()
	
class Database(object):
	def __init__(self, path=None):
		self.path = path
		if not os.path.isfile(self.path):
			self._create_db()
			
	def get_connection(self):
		conn = sqlite3.connect(self.path)
		conn.row_factory = Row
		return conn
		
class RLSDatabase(Database):
	def __init__(self):
		self.log = log.get_log("rls_database")
		home = config.get_home()
		path = os.path.join(home, "var", "rls.db")
		Database.__init__(self, path)
		
	@with_connection
	def _create_db(self, conn):
		self.log.info("Creating database")
		conn.executescript("""
			create table map (
				lfn text,
				pfn text,
				primary key (lfn, pfn)
			);
		""")
		
	@with_connection
	def add(self, conn, lfn, pfn):
		cur = conn.cursor()
		rec = {
			"lfn": lfn,
			"pfn": pfn
		}
		cur.execute("insert into map (lfn, pfn) values (:lfn,:pfn)", rec)
		cur.close()
		
	@with_connection
	def delete(self, conn, lfn, pfn=None):
		cur = conn.cursor()
		if pfn is None:
			cur.execute("delete from map where lfn=?",(lfn,))
		else:
			cur.execute("delete from map where lfn=? and pfn=?",(lfn,pfn))
		cur.close()
		
	@with_connection
	def lookup(self, conn, lfn):
		cur = conn.cursor()
		cur.execute("select pfn from map where lfn=?",(lfn,))
		pfns = []
		for row in cur.fetchall():
			pfns.append(row['pfn'])
		cur.close()
		return pfns

class CacheDatabase(Database):
	def __init__(self):
		self.log = log.get_log("cache_database")
		home = config.get_home()
		path = os.path.join(home, "var", "cache.db")
		Database.__init__(self, path)
		
	@with_connection
	def _create_db(self, conn):
		self.log.info("Creating database")
		conn.executescript("""
			create table cache (
				lfn text,
				uuid text,
				status text,
				primary key (lfn)
			);
		""")
		
	@with_connection
	def lookup(self, conn, lfn):
		cur = conn.cursor()
		cur.execute("select uuid from cache where lfn=?",(lfn,))
		row = cur.fetchone()
		cur.close()
		if row is None:
			raise Exception("%s not found" % lfn)
		return row['uuid']
		
	@with_connection
	def ready(self, conn, lfn):
		cur = conn.cursor()
		cur.execute("select status from cache where lfn=?",(lfn,))
		row = cur.fetchone()
		cur.close()
		if row is None:
			raise Exception("%s not found" % lfn)
		return row['status'] == 'ready'
		
	@with_connection
	def cached(self, conn, lfn):
		cur = conn.cursor()
		cur.execute("select lfn from cache where lfn=?",(lfn,))
		result = cur.fetchone()
		cur.close()
		return result is not None
		
	@with_connection
	def insert(self, conn, lfn):
		cur = conn.cursor()
		cur.execute("""
			insert into cache (lfn, status) 
			values (?,'unready')""",(lfn,))
		cur.close()
		
	@with_connection
	def update(self, conn, lfn, uuid):
		cur = conn.cursor()
		cur.execute("""
			update cache 
			set status='ready',uuid=? 
			where lfn=?""", (uuid,lfn))
		cur.close()
