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
import signal
from optparse import OptionParser
from xmlrpclib import ServerProxy

from mule import config, log, util, server
from mule import bdb as db

RLS_PORT = 3880

def connect(host='localhost', port=RLS_PORT):
	uri = "http://%s:%s" % (host,port)
	return ServerProxy(uri, allow_none=True)

class RLS(object):
	def __init__(self):
		self.log = log.get_log("rls")
		self.server = server.MuleServer('', RLS_PORT)
		
	def stop(self, signum=None, frame=None):
		self.log.info("Shutting down RLS...")
		self.db.close()
		sys.exit(0)
			
	def run(self):
		try:
			self.log.info("Starting RLS...")
			self.db = db.RLSDatabase()
			signal.signal(signal.SIGTERM, self.stop)
			self.server.register_function(self.lookup)
			self.server.register_function(self.multilookup)
			self.server.register_function(self.add)
			self.server.register_function(self.multiadd)
			self.server.register_function(self.delete)
			self.server.register_function(self.multidelete)
			self.server.register_function(self.ready)
			self.server.register_function(self.clear)
			self.server.serve_forever()
		except KeyboardInterrupt:
			self.stop()
			
	def lookup(self, lfn):
		"""
		Look up all the pfns for lfn
		"""
		self.log.debug("lookup %s" % lfn)
		return self.db.lookup(lfn)
		
	def multilookup(self, lfns):
		"""
		Look up all the pfns for a set of lfns
		"""
		self.log.debug("multilookup %d" % len(lfns))
		results = {}
		for lfn in lfns:
			results[lfn] = self.db.lookup(lfn)
		return results
		
	def add(self, lfn, pfn):
		"""
		Add a mapping
		"""
		self.log.debug("add %s %s" % (lfn, pfn))
		self.db.add(lfn, pfn)
		
	def multiadd(self, mappings):
		"""
		Add a list of mappings
		"""
		self.log.debug("multiadd %d" % len(mappings))
		for lfn, pfn in mappings:
			self.db.add(lfn, pfn)
		
	def delete(self, lfn, pfn=None):
		"""
		Delete a mapping
		"""
		self.log.debug("delete %s %s" % (lfn, pfn))
		self.db.delete(lfn, pfn)
		
	def multidelete(self, mappings):
		"""
		Delete a list of mappings
		"""
		self.log.debug("multidelete %d" % len(mappings))
		for lfn, pfn in mappings:
			self.db.delete(lfn, pfn)
		
	def ready(self):
		"""
		This is just so that the agent can tell 
		if the RLS server is running
		"""
		return True
		
	def clear(self):
		"""Clear all entries from db"""
		self.db.clear()
		
def main():
	parser = OptionParser()
	parser.add_option("-f", "--foreground", action="store_true", 
		dest="foreground", default=False,
		help="Do not fork [default: %default]")

	(options, args) = parser.parse_args()
	
	if len(args) > 0:
		parser.error("Invalid argument")
	
	# Fork
	if not options.foreground:
		util.daemonize()
	
	os.chdir(config.get_home())
	
	# Configure logging (after the fork)
	log.configure()
	
	l = log.get_log("rls")
	try:
		r = RLS()
		r.run()
	except Exception, e:
		l.exception(e)
		sys.exit(1)
	
if __name__ == '__main__':
	main()