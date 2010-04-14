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
from optparse import OptionParser

from mule import cache

SYMLINK = os.getenv("MULE_SYMLINK","false").lower() == "true"
RENAME = os.getenv("MULE_RENAME","false").lower() == "true"

def timed(function):
	def timer(*args, **kwargs):
		try:
			start = time.time()
			return function(*args, **kwargs)
		finally:
			end = time.time()
			sys.stderr.write("Called %s in %f seconds\n" % (function.__name__, end-start))
	return timer

@timed
def get(lfn, path, symlink):
	# If we already have path, then skip it
	if os.path.exists(path):
		sys.stderr.write("Path %s already exists\n" % path)
		return
	
	if not os.path.isabs(path):
		path = os.path.abspath(path)
		
	conn = cache.connect()
	sleeptime = 1
	while True:
		status = conn.get(lfn, path, symlink)
		if status == 'unready':
			sys.stderr.write("%s not ready: retrying in %d\n" % (lfn, sleeptime))
			time.sleep(sleeptime)
			sleeptime = min(sleeptime+1, 5)
		elif status == 'ready':
			break
		else:
			raise Exception("Unrecognized status: %s\n" % status)

@timed	
def put(path, lfn, rename):
	# If the path doesn't exist, then skip it
	if not os.path.exists(path):
		sys.stderr.write("Path %s does not exist\n" % path)
		return
		
	if not os.path.isabs(path):
		path = os.path.abspath(path)
	
	conn = cache.connect()
	conn.put(path, lfn, rename)

@timed
def remove(lfn, force):
	conn = cache.connect()
	conn.remove(lfn, force)

@timed
def ls():
	conn = cache.connect()
	results = conn.list()
	for rec in results:
		print rec['lfn'], rec['status'], rec['uuid']

@timed
def rls_add(lfn, pfn):
	conn = cache.connect()
	conn.rls_add(lfn, pfn)
	
@timed
def rls_lookup(lfn):
	conn = cache.connect()
	pfns = conn.rls_lookup(lfn)
	for pfn in pfns:
		print pfn

@timed	
def rls_delete(lfn, pfn):
	conn = cache.connect()
	conn.rls_delete(lfn, pfn)
	
def usage():
	sys.stderr.write("Usage: %s COMMAND\n" % os.path.basename(sys.argv[0]))
	sys.stderr.write("""
Commands:
   get LFN PATH     Download LFN and store it at PATH
   put PATH LFN     Upload PATH to LFN
   remove LFN       Remove LFN from cache
   list             List cache contents
   rls_add LFN PFN  Add mapping to RLS
   rls_delete LFN   Remove mappings for LFN from RLS
   rls_lookup LFN   List RLS mappings for LFN
   help             Display this message
""")
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
	elif cmd in ['remove','rm']:
		parser = OptionParser("Usage: %prog remove [options] LFN")
		parser.add_option("-f", "--force", action="store_true",
			dest="force", default=False,
			help="Force LFN to be removed from cache [default: %default]")
		(options, args) = parser.parse_args(args=args)
		if len(args) != 1:
			parser.error("Specify LFN")
		lfn = args[0]
		remove(lfn, options.force)
	elif cmd in ['list','ls']:
		parser = OptionParser("Usage: %prog list")
		(options, args) = parser.parse_args(args=args)
		if len(args) > 0:
			parser.error("Invalid argument")
		ls()
	elif cmd in ['rls_add','add']:
		parser = OptionParser("Usage: %prog rls_add LFN PFN")
		(options, args) = parser.parse_args(args=args)
		if len(args) != 2:
			parser.error("Specify LFN and PFN")
		lfn = args[0]
		pfn = args[1]
		rls_add(lfn, pfn)
	elif cmd in ['rls_lookup','rls_lu','lookup','lu']:
		parser = OptionParser("Usage: %prog rls_lookup LFN")
		(options, args) = parser.parse_args(args=args)
		if len(args) != 1:
			parser.error("Specify LFN")
		lfn = args[0]
		rls_lookup(lfn)
	elif cmd in ['rls_delete','rls_del','delete','del']:
		parser = OptionParser("Usage: %prog rls_del LFN [PFN]")
		(options, args) = parser.parse_args(args=args)
		if len(args) not in [1,2]:
			parser.error("Specify LFN and/or PFN")
		lfn = args[0]
		if len(args) > 1:
			pfn = args[1]
		else:
			pfn = None
		rls_delete(lfn, pfn)
	elif cmd in ['-h','help','-help','--help']:
		usage()
	else:
		sys.stderr.write("Unrecognized argument: %s\n" % cmd)
	
	
if __name__ == '__main__':
	main()
