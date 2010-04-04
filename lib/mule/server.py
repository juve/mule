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
from SocketServer import ThreadingMixIn
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

from mule import log

class MuleRequestHandler(SimpleXMLRPCRequestHandler):
	def __init__(self, request, client_address, server):
		self.log = log.get_log("client %s:%d" % client_address)
		SimpleXMLRPCRequestHandler.__init__(self, request, client_address, server)
		
	def log_error(self, format, *args):
		self.log.error(format % args)
		
	def log_message(self, format, *args):
		self.log.debug(format % args)
		
class MuleServer(ThreadingMixIn, SimpleXMLRPCServer):
	def __init__(self, host, port, requestHandler=MuleRequestHandler):
		SimpleXMLRPCServer.__init__(self, (host, port), requestHandler=requestHandler, 
									allow_none=True, encoding=None, logRequests=True)