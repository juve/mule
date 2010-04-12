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
import time
import cPickle as pickle
from threading import Thread
from mule import log, config

try:
	import bsddb3.db as bdb
except ImportError:
	import bsddb.db as bdb

version = tuple([int(y) for y in bdb.__version__.split(".")])
if version < (4,7):
	raise ImportError('bsddb version 4.7 or later required')
del version

def with_transaction(method):
	def with_transaction(self, *args, **kwargs):
		if len(args)>0 and isinstance(args[0],Database):
			return method(self, *args, **kwargs)
		else:
			txn = self.env.txn_begin()
			try:
				result = method(self, txn, *args, **kwargs)
				txn.commit()
				return result
			except:
				txn.abort()
				raise
	return with_transaction

class CheckpointThread(Thread):
	def __init__(self, env, interval=300):
		Thread.__init__(self)
		self.setDaemon(True)
		self.log = log.get_log("ckpt_thread")
		self.env = env
		self.interval = interval
	
	def run(self):
		while True:
			try:
				time.sleep(self.interval)
				self.log.info("checkpointing database")
				self.env.txn_checkpoint(0,0)
			except:
				t, e, tb = sys.exc_info()
				self.log.exception(e)	

class Database(object):
	max_txns = 1000
	
	def __init__(self, path, name, duplicates=False):
		self.path = path
		self.dbpath = os.path.join(self.path, name)
		
		if not os.path.isdir(self.path):
			os.makedirs(self.path)
		
		self.env = bdb.DBEnv()
		self.env.set_tx_max(self.max_txns)
		self.env.set_lk_max_lockers(self.max_txns*2)
		self.env.set_lk_max_locks(self.max_txns*2)
		self.env.set_lk_max_objects(self.max_txns*2)
		self.env.set_flags(bdb.DB_TXN_NOSYNC, True)
		if bdb.version() > (4,7):
			self.env.log_set_config(bdb.DB_LOG_AUTO_REMOVE, True)
		self.env.open(self.path, bdb.DB_CREATE | bdb.DB_INIT_LOCK | 
				bdb.DB_INIT_LOG | bdb.DB_INIT_MPOOL | bdb.DB_INIT_TXN | 
				bdb.DB_RECOVER | bdb.DB_THREAD)
		
		self.db = bdb.DB(self.env)
		if duplicates:
			self.db.set_flags(bdb.DB_DUPSORT)
		if bdb.version() > (4,1):
			txn = self.env.txn_begin()
			self.db.open(self.dbpath, name, flags=bdb.DB_CREATE|bdb.DB_THREAD, dbtype=bdb.DB_BTREE, txn=txn)
			txn.commit()
		else:
			self.db.open(self.dbpath, name, flags=bdb.DB_CREATE|bdb.DB_THREAD, dbtype=bdb.DB_BTREE)
	
	def close(self):
		self.env.log_flush()
		self.db.close()
		self.env.close()
		
class RLSDatabase(Database):
	def __init__(self):
		self.log = log.get_log("rls_database")
		home = config.get_home()
		path = os.path.join(home, "var", "rls")
		Database.__init__(self, path, "rls", duplicates=True)
		
	@with_transaction
	def add(self, txn, lfn, pfn):
		cur = self.db.cursor(txn)
		try:
			current = cur.get_both(lfn, pfn)
			if current is None:
				cur.put(lfn, pfn, flags=bdb.DB_KEYLAST)
		finally:
			cur.close()
	
	@with_transaction
	def delete(self, txn, lfn, pfn=None):
		cur = self.db.cursor(txn)
		try:
			if pfn is None:
				current = cur.set(lfn)
				while current is not None:
					cur.delete()
					current = cur.next_dup()
			else:
				current = cur.set_both(lfn, pfn)
				if current is not None:
					cur.delete()
		finally:
			cur.close()
			
	def lookup(self, lfn):
		cur = self.db.cursor()
		try:
			result = []
			current = cur.set(lfn)
			while current is not None:
				result.append(current[1])
				current = cur.next_dup()
			return result
		finally:
			cur.close()
		
class CacheDatabase(Database):
	def __init__(self):
		self.log = log.get_log("cache_database")
		home = config.get_home()
		path = os.path.join(home, "var", "cache")
		Database.__init__(self, path, "cache", duplicates=False)
	
	def get(self, lfn):
		cur = self.db.cursor()
		try:
			current = cur.set(lfn)
			if current is not None:
				return pickle.loads(current[1])
			else:
				return None
		finally:
			cur.close()
		
	@with_transaction
	def put(self, txn, lfn):
		next = { 'status': 'unready', 'uuid': None }
		self.db.put(lfn, pickle.dumps(next), txn, bdb.DB_NOOVERWRITE)
			
	@with_transaction
	def remove(self, txn, lfn):
		self.db.delete(lfn, txn)
		
	def list(self):
		cur = self.db.cursor()
		try:
			result = []
			current = cur.first()
			while current is not None:
				rec = pickle.loads(current[1])
				rec['lfn'] = current[0]
				result.append(rec)
				current = cur.next()
			return result
		finally:
			cur.close()
					
	@with_transaction
	def update(self, txn, lfn, status, uuid):
		next = { 'status': status, 'uuid': uuid }
		self.db.put(lfn, pickle.dumps(next), txn)


if __name__ == '__main__':
	log.configure()
	db = CacheDatabase()
	try:
		if db.put("b"):
			rec = db.get("b")
			if rec['status'] == "unready":
				db.update("b", "123123123123")
		print db.get("b")	
	finally:
		db.close()
