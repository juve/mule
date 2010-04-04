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

from mule import agent
	
def get(lfn, path, symlink=True):
	# If we already have path, then skip it
	if os.path.exists(path):
		print "Path %s already exists" % path
		return
	
	if not os.path.isabs(path):
		path = os.path.abspath(path)
		
	conn = agent.connect()
	conn.get(lfn, path, symlink)
	
def put(path, lfn):
	# If the path doesn't exist, then skip it
	if not os.path.exists(path):
		print "Path %s does not exist" % path
		return
		
	if not os.path.isabs(path):
		path = os.path.abspath(path)
		
	conn = agent.connect()
	conn.put(path, lfn)

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
		if len(args) != 2:
			print "Specify LFN and PATH"
			sys.exit(1)
		lfn = args[0]
		path = args[1]
		get(lfn, path)
	elif cmd in ['put']:
		if len(args) != 2:
			print "Specify PATH and LFN"
			sys.exit(1)
		path = args[0]
		lfn = args[1]
		put(path, lfn)
	elif cmd in ['del']:
		if len(args) != 1:
			print "Specify LFN"
			sys.exit(1)
		lfn = args[0]
		delete(lfn)
	elif cmd in ['-h','help','-help','--help']:
		usage()
	else:
		print "Unrecognized argument: %s" % cmd
	
	
if __name__ == '__main__':
	main()