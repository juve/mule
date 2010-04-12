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
import urllib
from threading import Lock, Thread
from optparse import OptionParser
from xmlrpclib import ServerProxy
from uuid import uuid4

from mule import config, log, util, rls, server
from mule import bdb as db

BLOCK_SIZE = int(os.getenv("MULE_BLOCK_SIZE", 64*1024))
DEFAULT_CACHE = os.getenv("MULE_CACHE", "/tmp/mule")
DEFAULT_RLS = os.getenv("MULE_RLS")

AGENT_PORT = 3881

def connect(host='localhost',port=AGENT_PORT):
	"""
	Connect to the agent server running at host:port
	"""
	uri = "http://%s:%s" % (host, port)
	return ServerProxy(uri, allow_none=True)
	
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
		f = urllib.urlopen(url)
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
		
class DownloadThread(Thread):
	def __init__(self, function):
		Thread.__init__(self)
		self.log = log.get_log("download thread")
		self.setDaemon(True)
		self.function = function
		
	def run(self):
		try:
			self.function()
		except Exception, e:
			self.log.exception(e)
		
class AgentHandler(server.MuleRequestHandler):
	def do_GET(self):
		head, uuid = os.path.split(self.path)
		path = self.server.agent.get_cfn(uuid)
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
		
class Agent(object):
	def __init__(self, rls_host, cache_dir, hostname=fqdn()):
		self.log = log.get_log("agent")
		self.rls_host = rls_host
		self.cache_dir = cache_dir
		self.hostname = hostname
		self.server = server.MuleServer('', AGENT_PORT,
		                                requestHandler=AgentHandler)
		self.server.agent = self
		self.lock = Lock()
		
	def stop(self, signum=None, frame=None):
		self.log.info("Stopping agent...")
		self.db.close()
		sys.exit(0)
	
	def run(self):
		try:
			self.log.info("Starting agent...")
			self.db = db.CacheDatabase()
			signal.signal(signal.SIGTERM, self.stop)
			self.server.register_function(self.get)
			self.server.register_function(self.put)
			self.server.register_function(self.remove)
			self.server.register_function(self.list)
			self.server.register_function(self.rls_delete)
			self.server.register_function(self.rls_add)
			self.server.register_function(self.rls_lookup)
			self.server.serve_forever()
		except KeyboardInterrupt:
			self.stop()
			
	def get_uuid(self):
		"""
		Generate a unique ID
		"""
		return str(uuid4())
		
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
		return "http://%s:%s/%s" % (self.hostname, AGENT_PORT, uuid)
		
	def get(self, lfn, path, symlink=True):
		"""
		Get lfn and store it at path
		"""
		self.log.debug("get %s %s" % (lfn, path))
		
		# Check path
		if os.path.exists(path):
			raise Exception("%s already exists" % path)
		
		# Check to see if the file is cached
		rec = self.db.get(lfn)
		
		# If it isn't in cache, try to get it
		if rec is None:
			thread = None
			self.lock.acquire()
			try:
				rec = self.db.get(lfn)
				if rec is None:
					self.db.put(lfn)
					def get_file():
						try:
							self.do_get(lfn, path, symlink)
						except Exception, e:
							self.log.exception(e)
							self.db.update(lfn, 'failed', None)
					thread = DownloadThread(get_file)
					thread.start()
			finally:
				self.lock.release()
			
			# If we launched a thread, wait up to 5 seconds
			if thread is not None:
				thread.join(5)
			
			# Retrieve the latest status
			rec = self.db.get(lfn)
		
		# If it is ready, then copy it to path
		if rec['status'] == 'ready':
			cfn = self.get_cfn(rec['uuid'])
			self.log.debug("Copying %s to %s" % (cfn, path))
			if not os.path.exists(cfn):
				raise Exception("%s was not found in cache" % cfn)
			if symlink:
				os.symlink(cfn, path)
			else:
				copy(cfn, path)
		
		# If it failed, then raise an exception
		if rec['status'] == 'failed':
			raise Exception("Unable to get %s: failed" % lfn)
		
		# Otherwise it isn't ready, just return the current status
		return rec['status']
	
	def do_get(self, lfn, path, symlink):
		
		# Lookup lfn
		conn = rls.connect(self.rls_host)
		pfns = conn.lookup(lfn)
		
		# As a last resort, get it from the source if the source is a URL
		if lfn.startswith('http'):
			pfns.append(lfn)
				
		# If not found in RLS
		if len(pfns) == 0:
			raise Exception('%s does not exist in RLS' % lfn)
		
		# Create new name
		uuid = self.get_uuid()
		cfn = self.get_cfn(uuid)
		pfn = self.get_pfn(uuid)
			
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
		
		if success:
			conn.add(lfn, pfn)
			self.db.update(lfn, 'ready', uuid)
		else:
			raise Exception('Unable to get %s: all pfns failed' % lfn)
		
	def put(self, path, lfn, rename=True):
		"""
		Put path into cache and register lfn
		"""
		self.log.debug("put %s %s" % (path, lfn))
		
		# Check for path
		if not os.path.exists(path):
			raise Exception("%s does not exist", path)
		
		# If its already in cache, then return
		if self.db.get(lfn) is not None:
			self.log.error("%s already cached" % lfn)
			return
		
		# Create new names
		uuid = self.get_uuid()
		cfn = self.get_cfn(uuid)
		pfn = self.get_pfn(uuid)
		
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
		self.db.update(lfn, 'ready', uuid)
		
		# Register lfn->pfn
		conn = rls.connect(self.rls_host)
		conn.add(lfn, pfn)
		
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
		
		uuid = rec['uuid']
		if uuid is not None:
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
		
def main():
	parser = OptionParser()
	parser.add_option("-f", "--foreground", action="store_true", 
		dest="foreground", default=False,
		help="Do not fork [default: fork]")
	parser.add_option("-r", "--rls", action="store", dest="rls",
		default=DEFAULT_RLS, metavar="HOST",
		help="RLS host [def: %default]")
	parser.add_option("-c", "--cache", action="store", dest="cache",
		default=DEFAULT_CACHE, metavar="DIR",
		help="Cache directory [def: %default]")

	(options, args) = parser.parse_args()
	
	if len(args) > 0:
		parser.error("Invalid argument")
	
	if not options.rls:
		parser.error("Specify --rls or MULE_RLS environment")
	
	if os.path.isfile(options.cache):
		parser.error("--cache argument is file")
		
	if not os.path.isdir(options.cache):
		os.makedirs(options.cache)
		
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
	
	l = log.get_log("agent")
	try:
		a = Agent(options.rls, options.cache)
		a.run()
	except Exception, e:
		l.exception(e)
		sys.exit(1)
	
if __name__ == '__main__':
	main()
