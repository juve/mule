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

from mule import agent

SYMLINK = os.getenv("MULE_SYMLINK","false").lower() == "true"
RENAME = os.getenv("MULE_RENAME","false").lower() == "true"
	
def get(lfn, path, symlink):
	# If we already have path, then skip it
	if os.path.exists(path):
		print "Path %s already exists" % path
		return
	
	if not os.path.isabs(path):
		path = os.path.abspath(path)
		
	conn = agent.connect()
	conn.get(lfn, path, symlink)
	
def put(path, lfn, rename):
	# If the path doesn't exist, then skip it
	if not os.path.exists(path):
		print "Path %s does not exist" % path
		return
		
	if not os.path.isabs(path):
		path = os.path.abspath(path)
		
	conn = agent.connect()
	conn.put(path, lfn, rename)

def delete(lfn):
	conn = agent.connect()
	conn.delete(lfn)
	
def usage():
	print "Usage: %s COMMAND" % os.path.basename(sys.argv[0])
	print ""
	print "Commands:"
	print "   get LFN PATH   Download LFN and store it at PATH"
	print "   put PATH LFN   Upload PATH to LFN"
	print "   del LFN        Remove LFN"
	print "   help           Display this message"
	sys.exit(1)
	
def main():
	if len(sys.argv) < 2:
		usage()
		
	cmd = sys.argv[1]
	args = sys.argv[2:]
	
	if cmd in ['get']:
		parser = OptionParser("Usage: %prog get LFN PATH")
		parser.add_option("-s", "--symlink", action="store_true", 
			dest="symlink", default=SYMLINK,
			help="symlink PATH to cached file [default: %default]")
		(options, args) = parser.parse_args(args=args)
		if len(args) != 2:
			parser.error("Specify LFN and PATH")
		lfn = args[0]
		path = args[1]
		get(lfn, path, options.symlink)
	elif cmd in ['put']:
		parser = OptionParser("Usage: %prog put PATH LFN")
		parser.add_option("-r", "--rename", action="store_true", 
			dest="rename", default=RENAME,
			help="rename PATH to cached file [default: %default]")
		(options, args) = parser.parse_args(args=args)
		if len(args) != 2:
			pasrser.error("Specify PATH and LFN")
		path = args[0]
		lfn = args[1]
		put(path, lfn, options.rename)
	elif cmd in ['del']:
		parser = OptionParser("Usage: %prog del LFN")
		(options, args) = parser.parse_args(args=args)
		if len(args) != 1:
			parser.error("Specify LFN")
		lfn = args[0]
		delete(lfn)
	elif cmd in ['-h','help','-help','--help']:
		usage()
	else:
		print "Unrecognized argument: %s" % cmd
	
	
if __name__ == '__main__':
	main()