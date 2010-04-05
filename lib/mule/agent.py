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
import socket
import time
import urllib
from optparse import OptionParser
from xmlrpclib import ServerProxy
from uuid import uuid4

from mule import config, log, util, rls, db, server

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
		self.db = db.CacheDatabase()
		self.server = server.MuleServer('localhost', AGENT_PORT,
		                                requestHandler=AgentHandler)
		self.server.agent = self
	
	def run(self):
		try:
			self.log.info("Starting agent...")
			self.server.register_function(self.get)
			self.server.register_function(self.put)
			self.server.register_function(self.delete)
			self.server.serve_forever()
		except KeyboardInterrupt:
			sys.exit(0)
			
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
		self.log.info("get %s %s" % (lfn, path))
		
		# Check path
		if os.path.exists(path):
			raise Exception("%s already exists" % path)
		
		# Try to insert a new record
		try:
			# If it succeeds, then we need to get it
			self.db.insert(lfn)
			exists = False
		except Exception, e:
			# If it fails then the file is in the cache already
			exists = True
		
		if exists:
			# Another thread is downloading it
			# Wait for the other thread to finish
			i = 1
			while not self.db.ready(lfn):
				self.log.info("waiting for %s" % lfn)
				time.sleep(5)
				i += 1
				if i > 60:
					raise Exception("timeout waiting for %s" % lfn)
			uuid = self.db.lookup(lfn)
			cfn = self.get_cfn(uuid)
		else:
			# Lookup lfn
			conn = rls.connect(self.rls_host)
			pfns = conn.lookup(lfn)
		
			# Create new name
			uuid = self.get_uuid()
			cfn = self.get_cfn(uuid)
			pfn = self.get_pfn(uuid)
			
			# Create dir if needed
			d = os.path.dirname(cfn)
			if not os.path.exists(d):
				os.makedirs(d)
			
			# Download
			for p in pfns:
				# Download from peer to cache
				try:
					download(p, cfn)
					break
				except Exception, e:
					self.log.exception(e)
		
			# Update cache db		
			self.db.update(lfn, uuid)
			
			# Register lfn->pfn
			conn.add(lfn, pfn)
			
		# Validate cached copy
		if not os.path.exists(cfn):
			raise Exception("%s was not found in cache" % cfn)
		
		# Create link or copy to path
		if symlink: os.symlink(cfn, path)
		else: copy(cfn, path)
		
	def put(self, path, lfn, rename=True):
		"""
		Put path into cache and register lfn
		"""
		self.log.info("put %s %s" % (path, lfn))
		
		# Check for path
		if not os.path.exists(path):
			raise Exception("%s does not exist", path)
		
		# If its already in cache, then return
		if self.db.cached(lfn):
			self.log.info("%s already cached" % lfn)
			return
		
		# Make sure RLS is available
		conn = rls.connect(self.rls_host)
		conn.ready()
		
		# Create new names
		uuid = self.get_uuid()
		cfn = self.get_cfn(uuid)
		pfn = self.get_pfn(uuid)
		
		# Create dir if needed
		d = os.path.dirname(cfn)
		if not os.path.exists(d):
			os.makedirs(d)
		
		# Create an entry in the cache db
		self.db.insert(lfn)
		
		# Move path to cache
		if rename: os.rename(path, cfn)
		else: copy(path, cfn)
		
		# Update the cache db
		self.db.update(lfn, uuid)
		self.log.info("%s stored as %s" % (lfn, uuid))
		
		# Register lfn->pfn
		conn.add(lfn, pfn)
		
	def delete(self, lfn, pfn=None):
		"""
		Delete lfn->pfn mapping
		"""
		self.log.info("delete %s %s" % (lfn, pfn))
		conn = rls.connect(self.rls_host)
		rls.delete(lfn, pfn)
			
def main():
	home = config.get_home()
	default_cache = os.path.join(home,"var","cache")
	default_cache = os.getenv("MULE_CACHE", default_cache)
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
