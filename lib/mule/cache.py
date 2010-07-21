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
import errno
import signal
import socket
import time
import urllib2
import hashlib
from threading import Lock, Thread, Event
from Queue import Queue
from optparse import OptionParser
from xmlrpclib import ServerProxy

from mule import config, log, util, rls, server
from mule import bdb as db

BLOCK_SIZE = int(os.getenv("MULE_BLOCK_SIZE", 64*1024))
DEFAULT_DIR = os.getenv("MULE_CACHE_DIR", "/tmp/mule")
DEFAULT_RLS = os.getenv("MULE_RLS")

CACHE_PORT = 3881

def connect(host='localhost',port=CACHE_PORT):
	"""
	Connect to the cache server running at host:port
	"""
	uri = "http://%s:%s" % (host, port)
	return ServerProxy(uri, allow_none=True)

def num_cpus():
	# Python 2.6+
	try:
		import multiprocessing
		return multiprocessing.cpu_count()
	except (ImportError,NotImplementedError):
		pass

	# POSIX
	try:
		res = int(os.sysconf('SC_NPROCESSORS_ONLN'))
		if res > 0: return res
	except (AttributeError,ValueError):
		pass
		
	return 1
	
def fqdn():
	"""
	Get the fully-qualified domain name of this host
	"""
	hostname = socket.gethostname()
	return socket.getfqdn(hostname)

def copy(src, dest):
	"""
	Copy file src to file dest
	"""
	f = None
	g = None
	try:
		f = open(src,"rb")
		g = open(dest,"wb")
		copyobj(f,g)
	finally:
		if f: f.close()
		if g: g.close()

def copyobj(src, dest):
	"""
	Copy file-like object src to file-like object dest
	"""
	while 1:
		buf = src.read(BLOCK_SIZE)
		if not buf: break
		dest.write(buf)
		
def download(url, path):
	"""
	Download url and store it at path
	"""
	f = None
	g = None
	try:
		f = urllib2.urlopen(url)
		g = open(path, 'wb')
		copyobj(f, g)
	finally:
		if f: f.close()
		if g: g.close()
		
def ensure_path(path):
	"""
	Create path if it doesn't exist
	"""
	if not os.path.exists(path):
		try:
			os.makedirs(path)
		except OSError, e:
			if e.errno != errno.EEXIST:
				raise

class Statistic(object):
	def __init__(self, value=0):
		self.lock = Lock()
		self._value = value
		
	def increment(self, i=1):
		self.lock.acquire()
		try:
			self._value += i
		finally:
			self.lock.release()
	
	def value(self):
		return self._value
		
class Statistics(object):
	def __init__(self):
		self.since = time.ctime()
		self.gets = Statistic()
		self.puts = Statistic()
		self.hits = Statistic()
		self.misses = Statistic()
		self.near_misses = Statistic()
		self.failures = Statistic()
		self.duplicates = Statistic()
		
	def get_map(self):
		return {
			'since': self.since,
			'gets': self.gets.value(),
			'puts': self.puts.value(),
			'hits': self.hits.value(),
			'misses': self.misses.value(),
			'near_misses': self.near_misses.value(),
			'failures': self.failures.value(),
			'duplicates': self.duplicates.value()
		}
		
class DownloadRequest(object):
	def __init__(self, lfn, pfns):
		self.event = Event()
		self.lfn = lfn
		self.pfns = pfns
		self.exception = None

class DownloadThread(Thread):
	num = 1
	def __init__(self, cache):
		Thread.__init__(self)
		self.log = log.get_log("downloader %d" % DownloadThread.num)
		DownloadThread.num += 1
		self.setDaemon(True)
		self.cache = cache
		
	def run(self):
		while True:
			req = self.cache.queue.get()
			try:
				self.cache.fetch(req.lfn, req.pfns)
				self.cache.db.update(req.lfn, 'ready')
			except Exception, e:
				req.exception = e
				self.cache.db.update(req.lfn, 'failed')
			finally:
				req.event.set()
		
class CacheHandler(server.MuleRequestHandler):
	def do_GET(self):
		head, uuid = os.path.split(self.path)
		path = self.server.cache.get_cfn(uuid)
		f = None
		try:
			f = open(path, 'rb')
			fs = os.fstat(f.fileno())
			self.send_response(200)
			self.send_header("Content-type", "application/octet-stream")
			self.send_header("Content-Length", str(fs[6]))
			self.send_header("Last-Modified", 
							 self.date_time_string(fs.st_mtime))
			self.end_headers()
			copyobj(f, self.wfile)
		except IOError:
			self.send_error(404, "File not found")
		finally:
			if f: f.close()
		
class Cache(object):
	def __init__(self, rls_host, cache_dir, threads, hostname=fqdn()):
		self.log = log.get_log("cache")
		self.rls_host = rls_host
		self.cache_dir = cache_dir
		self.hostname = hostname
		self.st = Statistics()
		self.server = server.MuleServer('', CACHE_PORT,
		                                requestHandler=CacheHandler)
		self.server.cache = self
		self.lock = Lock()
		self.queue = Queue()
		for i in range(0, threads):
			t = DownloadThread(self)
			t.start()
					
	def stop(self, signum=None, frame=None):
		self.log.info("Stopping cache...")
		self.db.close()
		sys.exit(0)
	
	def run(self):
		try:
			self.log.info("Starting cache...")
			self.db = db.CacheDatabase()
			signal.signal(signal.SIGTERM, self.stop)
			self.server.register_function(self.get)
			self.server.register_function(self.multiget)
			self.server.register_function(self.put)
			self.server.register_function(self.multiput)
			self.server.register_function(self.remove)
			self.server.register_function(self.list)
			self.server.register_function(self.rls_delete)
			self.server.register_function(self.rls_add)
			self.server.register_function(self.rls_lookup)
			self.server.register_function(self.get_bloom_filter)
			self.server.register_function(self.stats)
			self.server.register_function(self.rls_clear)
			self.server.register_function(self.clear)
			self.server.serve_forever()
		except KeyboardInterrupt:
			self.stop()
			
	def get_uuid(self, lfn):
		"""
		Generate a unique ID for lfn
		"""
		return hashlib.sha1(lfn).hexdigest()
		
	def get_cfn(self, uuid):
		"""
		Generate a path for a given uuid in the cache
		"""
		l1 = uuid[0:2]
		l2 = uuid[2:4]
		return os.path.join(self.cache_dir, l1, l2, uuid)
		
	def get_pfn(self, uuid):
		"""
		Get a pfn for the given uuid
		"""
		return "http://%s:%s/%s" % (self.hostname, CACHE_PORT, uuid)
		
	def get(self, lfn, path, symlink=True):
		"""
		Get lfn and store it at path
		"""
		self.log.debug("get %s %s" % (lfn, path))
		self.multiget([[lfn, path]], symlink)
	
	def multiget(self, pairs, symlink=True):
		"""
		For each [lfn, path] pair get lfn and store at path
		"""
		created = []
		ready = []
		unready = []
		for lfn, path in pairs:
			self.st.gets.increment()
			rec = self.db.get(lfn)
			if rec is None:
				self.lock.acquire()
				try:
					rec = self.db.get(lfn)
					if rec is None:
						self.db.put(lfn)
						created.append((lfn,path))
						self.st.misses.increment()
					else:
						unready.append((lfn,path))
						self.st.near_misses.increment()
				finally:
					self.lock.release()
			elif rec['status'] == 'ready':
				ready.append((lfn,path))
				self.st.hits.increment()
			elif rec['status'] == 'unready':
				unready.append((lfn,path))
				self.st.near_misses.increment()
			elif rec['status'] == 'failed':
				self.st.failures.increment()
				raise Exception("Unable to get %s: failed" % lfn)
			else:
				raise Exception("Unrecognized status: %s" % rec['status'])
		
		conn = rls.connect(self.rls_host)
		
		if len(created) > 0:
			requests = []
			mappings = conn.multilookup([i[0] for i in created])
			for lfn, path in created:
				req = DownloadRequest(lfn, mappings[lfn])
				self.queue.put(req)
				requests.append(req)
		
		for lfn, path in ready:
			self.get_cached(lfn, path, symlink)
		
		if len(created) > 0:	
			mappings = []
			for req in requests:
				req.event.wait()
				if req.exception is None:
					uuid = self.get_uuid(req.lfn)
					pfn = self.get_pfn(uuid)
					mappings.append([req.lfn, pfn])
			
			if len(mappings) > 0:
				conn.multiadd(mappings)
				
			for req in requests:
				if req.exception:
					raise req.exception
					
			for lfn, path in created:
				unready.append((lfn, path))
			
		while len(unready) > 0:
			u = unready[:]
			unready = []
			for lfn, path in u:
				rec = self.db.get(lfn)
				if rec is None:
					raise Exception("Record disappeared for %s" % lfn)
				elif rec['status'] == 'ready':
					self.get_cached(lfn, path, symlink)
				elif rec['status'] == 'failed':
					self.st.failures.increment()
					raise Exception("Unable to get %s: failed" % lfn)
				else:
					unready.append((lfn, path))
			if len(unready) > 0:
				time.sleep(5)
	
	def get_cached(self, lfn, path, symlink=True):
		uuid = self.get_uuid(lfn)
		cfn = self.get_cfn(uuid)
		if not os.path.exists(cfn):
			raise Exception("%s was not found in cache" % (lfn))
		# This is to support nested directories inside working dirs
		ensure_path(os.path.dirname(path))
		if symlink:
			os.symlink(cfn, path)
		else:
			copy(cfn, path)
			
	def fetch(self, lfn, pfns):
		for protocol in ['http:','https:','file:','ftp:']:
			if lfn.startswith(protocol):
				pfns.append(lfn)
					
		if len(pfns) == 0:
			raise Exception("%s not found in RLS" % lfn)
		
		# Create new name
		uuid = self.get_uuid(lfn)
		cfn = self.get_cfn(uuid)
		if os.path.exists(cfn):
			self.log.warning("Duplicate uuid detected: %s" % uuid)
			
		# Create dir if needed
		d = os.path.dirname(cfn)
		ensure_path(d)
			
		# Download the file
		success = False
		for p in pfns:
			try:
				download(p, cfn)
				success = True
				break
			except Exception, e:
				self.log.exception(e)
		
		if not success:
			raise Exception('Unable to get %s: all pfns failed' % lfn)
		
	def put(self, path, lfn, rename=True):
		"""
		Put path into cache as lfn
		"""
		self.log.debug("put %s %s" % (path, lfn))
		self.multiput([[path, lfn]], rename)
		
	def multiput(self, pairs, rename=True):
		"""
		For all [path, lfn] pairs put path into the cache as lfn
		"""
		# Make sure the files exist
		for path, lfn in pairs:
			if not os.path.exists(path):
				raise Exception("%s does not exist", path)
		
		# Add them to the cache
		mappings = []
		for path, lfn in pairs:
			self.st.puts.increment()
			
			# If its already in cache, then skip it
			if self.db.get(lfn) is not None:
				self.log.warning("%s already cached" % lfn)
				self.st.duplicates.increment()
				continue
		
			# Create new names
			uuid = self.get_uuid(lfn)
			cfn = self.get_cfn(uuid)
			pfn = self.get_pfn(uuid)
			if os.path.exists(cfn):
				self.log.warning("Possible duplicate uuid detected: %s" % uuid)
		
			# Create dir if needed
			d = os.path.dirname(cfn)
			ensure_path(d)
		
			# Create an entry in the cache db
			self.db.put(lfn)
		
			# Move path to cache
			if rename:
				os.rename(path, cfn)
			else:
				copy(path, cfn)
		
			# Update the cache db
			self.db.update(lfn, 'ready')
		
			mappings.append([lfn, pfn])
		
		# Register lfn->pfn mappings
		conn = rls.connect(self.rls_host)
		conn.multiadd(mappings)
		
	def remove(self, lfn, force=False):
		"""
		Remove lfn from cache
		"""
		self.log.debug("remove %s" % lfn)
		rec = self.db.get(lfn)
		if rec is None:
			return
			
		if not force and rec['status'] != 'ready':
			raise Exception('Cannot remove %s' % lfn)
		
		# Remove from database
		self.db.remove(lfn)
		
		if rec['status'] == 'ready':
			uuid = self.get_uuid(lfn)
			
			# Remove RLS mapping
			pfn = self.get_pfn(uuid)
			conn = rls.connect(self.rls_host)
			conn.delete(lfn, pfn)

			# Remove cached copy
			cfn = self.get_cfn(uuid)
			if os.path.isfile(cfn):
				os.unlink(cfn)
		
	def list(self):
		"""
		List all cached files
		"""
		self.log.debug("list")
		return self.db.list()
		
	def rls_delete(self, lfn, pfn=None):
		"""
		Delete lfn->pfn mapping
		"""
		self.log.debug("delete %s %s" % (lfn, pfn))
		conn = rls.connect(self.rls_host)
		conn.delete(lfn, pfn)
		
	def rls_add(self, lfn, pfn):
		"""
		Add lfn->pfn mapping to rls
		"""
		self.log.debug("add %s %s" % (lfn, pfn))
		conn = rls.connect(self.rls_host)
		conn.add(lfn, pfn)
		
	def rls_lookup(self, lfn):
		"""
		Lookup RLS mappings for lfn
		"""
		self.log.debug("lookup %s" % lfn)
		conn = rls.connect(self.rls_host)
		return conn.lookup(lfn)
		
	def get_bloom_filter(self, m, k):
		"""
		Return a bloom filter containing all the lfns in the cache
		"""
		return self.db.get_bloom_filter(m, k).tobase64()
		
	def stats(self):
		"""
		Return the statistics for this cache
		"""
		return self.st.get_map()
		
	def clear(self):
		# Clear database
		self.db.clear()
		
		# Remove files in cache
		def remove_all(directory):
			for i in os.listdir(directory):
				path = os.path.join(directory, i)
				if os.path.isdir(path):
					remove_all(path)
				else:
					os.unlink(path)
		remove_all(self.cache_dir)
		
		# Clear stats
		self.st = Statistics()
		
	def rls_clear(self):
		self.log.debug("rls clear")
		conn = rls.connect(self.rls_host)
		conn.clear()
		
def main():
	parser = OptionParser()
	parser.add_option("-f", "--foreground", action="store_true", 
		dest="foreground", default=False,
		help="Do not fork [default: fork]")
	parser.add_option("-r", "--rls", action="store", dest="rls",
		default=DEFAULT_RLS, metavar="HOST",
		help="RLS host [default: %default]")
	parser.add_option("-d", "--dir", action="store", dest="cache_dir",
		default=DEFAULT_DIR, metavar="DIR",
		help="Cache directory [default: %default]")
	parser.add_option("-t", "--threads", action="store", dest="threads",
		default=num_cpus(), metavar="N",
		help="Number of download threads [default: %default]")

	(options, args) = parser.parse_args()
	
	if len(args) > 0:
		parser.error("Invalid argument")
	
	if not options.rls:
		parser.error("Specify --rls or MULE_RLS environment")
	
	if os.path.isfile(options.cache_dir):
		parser.error("--directory argument is a file")
		
	if not os.path.isdir(options.cache_dir):
		os.makedirs(options.cache_dir)
		
	# See if RLS is ready
	try:
		conn = rls.connect(options.rls)
		conn.ready()
	except Exception, e:
		print "WARNING: RLS is not ready"
		
	# Fork
	if not options.foreground:
		util.daemonize()
		
	os.chdir(config.get_home())
	
	# Configure logging (after the fork)
	log.configure()
	
	l = log.get_log("cache")
	try:
		a = Cache(options.rls, options.cache_dir, options.threads)
		a.run()
	except Exception, e:
		l.exception(e)
		sys.exit(1)
	
if __name__ == '__main__':
	main()
