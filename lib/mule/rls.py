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
from optparse import OptionParser
from xmlrpclib import ServerProxy

from mule import config, log, util, db, server

RLS_PORT = 3880

def connect(host='localhost', port=RLS_PORT):
	uri = "http://%s:%s" % (host,port)
	return ServerProxy(uri, allow_none=True)

class RLS(object):
	def __init__(self):
		self.log = log.get_log("rls")
		self.server = server.MuleServer('', RLS_PORT)
		self.db = db.RLSDatabase()
		
	def run(self):
		try:
			self.log.info("Starting RLS...")
			self.server.register_function(self.lookup)
			self.server.register_function(self.add)
			self.server.register_function(self.delete)
			self.server.register_function(self.ready)
			self.server.serve_forever()
		except KeyboardInterrupt:
			sys.exit(0)
			
	def lookup(self, lfn):
		"""
		Look up all the pfns for lfn
		"""
		self.log.info("lookup %s" % lfn)
		return self.db.lookup(lfn)
		
	def add(self, lfn, pfn):
		"""
		Add a mapping
		"""
		self.log.info("add %s %s" % (lfn, pfn))
		self.db.add(lfn, pfn)
		
	def delete(self, lfn, pfn=None):
		"""
		Delete a mapping
		"""
		self.log.info("delete %s %s" % (lfn, pfn))
		self.db.delete(lfn, pfn)
		
	def ready(self):
		"""
		This is just so that the agent can tell 
		if the RLS server is running
		"""
		return True
		
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