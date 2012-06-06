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
from mule import rls

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
	if not os.path.isabs(path):
		path = os.path.abspath(path)
		
	if os.path.exists(path):
		sys.stderr.write("Path %s already exists. Removing." % path)
		os.unlink(path)
		
	conn = cache.connect()
	conn.get(lfn, path, symlink)

@timed
def multiget(stream, symlink):
	pairs = []
	for l in stream.readlines():
		l = l.strip()
		if len(l)==0 or l.startswith('#'):
			continue
		lfn, path = l.split()

		if not os.path.isabs(path):
			path = os.path.abspath(path)
					
		if os.path.exists(path):
			sys.stderr.write("Path %s already exists. Removing." % path)
			os.unlink(path)
		
		pairs.append([lfn, path])
	
	conn = cache.connect()
	conn.multiget(pairs, symlink)
	
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
def multiput(stream, symlink):
	pairs = []
	for l in stream.readlines():
		l = l.strip()
		if len(l)==0 or l.startswith('#'):
			continue
		path, lfn = l.split()
		
		if not os.path.exists(path):
			sys.stderr.write("Path %s does not exists\n" % path)
			continue
		
		if not os.path.isabs(path):
			path = os.path.abspath(path)
			
		pairs.append([path, lfn])
	
	conn = cache.connect()
	conn.multiput(pairs, symlink)
	
@timed
def remove(lfn, force):
	conn = cache.connect()
	conn.remove(lfn, force)

@timed
def ls(host):
	conn = cache.connect(host=host)
	results = conn.list()
	for rec in results:
		print rec['lfn'], rec['status']

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
	
@timed
def rls_direct_add(rls_host, lfn, pfn):
	conn = rls.connect(rls_host)
	conn.add(lfn, pfn)

@timed
def rls_direct_add_bench(rls_host, prefix):
	conn = rls.connect(rls_host)
	for i in range(0, 1000):
		conn.add(prefix+str(i), prefix+str(i))

@timed
def rls_direct_lookup(rls_host, lfn):
	conn = rls.connect(rls_host)
	pfns = conn.lookup(lfn)
	for pfn in pfns:
		print pfn

@timed
def rls_direct_delete(rls_host, lfn, pfn):
	conn = rls.connect(rls_host)
	conn.delete(lfn, pfn)

@timed
def get_bloom_filter(m, k):
	conn = cache.connect()
	bloom = conn.get_bloom_filter(m, k)
	for i in range(0, len(bloom)):
		print 'BloomFilter%d = "%s"' % (i, bloom[i])
		
@timed
def stats(host):
	conn = cache.connect(host=host)
	st = conn.stats()
	for k in st:
		v = st[k]
		if isinstance(v, str):
			print '%s = "%s"' % (k, v)
		else: 
			print '%s = %s' % (k, v)
			
@timed
def clear(host):
	conn = cache.connect(host=host)
	conn.clear()
	
@timed
def rls_clear():
	conn = cache.connect()
	conn.rls_clear()
	
@timed
def rls_direct_clear(rls_host):
	conn = rls.connect(rls_host)
	conn.clear()

def usage():
	sys.stderr.write("Usage: %s COMMAND\n" % os.path.basename(sys.argv[0]))
	sys.stderr.write("""
Commands:
   get LFN PATH                            Download LFN and store it at PATH
   multiget                                Fetch multiple LFNs
   put PATH LFN                            Upload PATH to LFN
   multiput                                Upload multiple paths
   remove LFN                              Remove LFN from cache
   list                                    List cache contents
   rls_add LFN PFN                         Add mapping to RLS
   rls_delete LFN                          Remove mappings for LFN from RLS
   rls_lookup LFN                          List RLS mappings for LFN
   bloom                                   Retrieve base64-encoded bloom filter for cache
   stats                                   Display cache statistics
   clear                                   Clear all entries from cache
   rls_clear                               Clear all entries from RLS
   rls_direct_add RLSHOST LFN PFN          Add mapping to RLS w/o going through cache
   rls_direct_delete RLSHOST LFN           Remove mappings for LFN from RLS w/o going through cache
   rls_direct_lookup RLSHOST LFN           List RLS mappings for LFN w/o going through cache
   rls_direct_clear RLSHOST                Clear all entries from RLS w/o going through cache
   rls_direct_add_bench RLSHOST PREFIX     For benchmarking the RLS by sending it 1000 requests
   help                                    Display this message
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
	elif cmd in ['mget','multiget']:
		parser = OptionParser("Usage: %prog multiget < input")
		parser.add_option("-f", "--file", action="store", 
			dest="file", metavar="FILE", default=None,
			help="Read input from FILE [default: stdin]")
		parser.add_option("-s", "--symlink", action="store_true", 
			dest="symlink", default=SYMLINK,
			help="symlink PATH to cached file [default: %default]")
		(options, args) = parser.parse_args(args=args)
		if options.file:
			f = None
			try:
				f = open(options.file, 'r')
				multiget(f, options.symlink)
			finally:
				if f: f.close()
		else:
			multiget(sys.stdin, options.symlink)
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
	elif cmd in ['mput','multiput']:
		parser = OptionParser("Usage: %prog multiput < input")
		parser.add_option("-f", "--file", action="store", 
			dest="file", metavar="FILE", default=None,
			help="Read input from FILE [default: stdin]")
		parser.add_option("-r", "--rename", action="store_true", 
			dest="rename", default=RENAME,
			help="rename PATH to cached file [default: %default]")
		(options, args) = parser.parse_args(args=args)
		if options.file:
			f = None
			try:
				f = open(options.file, 'r')
				multiput(f, options.rename)
			finally:
				if f: f.close()
		else:
			multiput(sys.stdin, options.rename)
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
		parser.add_option("-H", "--host", action="store", type="string",
			dest="host", default="localhost",
			help="Host to connect to")
		(options, args) = parser.parse_args(args=args)
		if len(args) > 0:
			parser.error("Invalid argument")
		ls(options.host)
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
	elif cmd in ['rls_direct_add']:
		parser = OptionParser("Usage: %prog rls_direct_add RLS_HOST LFN PFN")
		(options, args) = parser.parse_args(args=args)
		if len(args) != 3:
			parser.error("Specify RLSHOST, LFN and PFN")
		rls_host = args[0]
		lfn = args[1]
		pfn = args[2]
		rls_direct_add(rls_host, lfn, pfn)
	elif cmd in ['rls_direct_add_bench']:
		parser = OptionParser("Usage: %prog rls_direct_add_bench RLS_HOST PREFIX")
		(options, args) = parser.parse_args(args=args)
		if len(args) != 2:
			parser.error("Specify RLSHOST and PREFIX")
		rls_host = args[0]
		prefix = args[1]
		rls_direct_add_bench(rls_host, prefix)
	elif cmd in ['rls_direct_lookup']:
		parser = OptionParser("Usage: %prog rls_direct_lookup RLSHOST LFN")
		(options, args) = parser.parse_args(args=args)
		if len(args) != 2:
			parser.error("Specify RLSHOST and LFN")
		rls_host = args[0]
		lfn = args[1]
		rls_direct_lookup(rls_host, lfn)
	elif cmd in ['rls_direct_delete']:
		parser = OptionParser("Usage: %prog rls_direct_delete RLSHOST LFN [PFN]")
		(options, args) = parser.parse_args(args=args)
		if len(args) not in [2,3]:
			parser.error("Specify RLSHOST, LFN and/or PFN")
		rls_host = args[0]
		lfn = args[1]
		if len(args) > 2:
			pfn = args[2]
		else:
			pfn = None
		rls_direct_delete(rls_host, lfn, pfn)
	elif cmd in ['bloom','bf','get_bloom','get_bloom_filter']:
		parser = OptionParser("Usage: %prog bloom")
		parser.add_option("-m", "--size", action="store", type="int",
			dest="m", metavar="M", default=36*1024*8,
			help="Size of bloom filter [default: 36*1024*8]")
		parser.add_option("-k", "--hashes", action="store", 
			dest="k", default=3, type="int",
			help="Number of hashes [default: 3]")
		(options, args) = parser.parse_args(args=args)
		if len(args) > 0:
			parser.error("Invalid argument")
		get_bloom_filter(options.m, options.k)
	elif cmd in ['stats','stat','st']:
		parser = OptionParser("Usage: %prog stats")
		parser.add_option("-H", "--host", action="store", type="string",
			dest="host", default="localhost",
			help="Host to connect to")
		(options, args) = parser.parse_args(args=args)
		if len(args) > 0:
			parser.error("Invalid argument")
		stats(options.host)
	elif cmd in ['clear']:
		parser = OptionParser("Usage: %prog clear")
		parser.add_option("-H", "--host", action="store", type="string",
			dest="host", default="localhost",
			help="Host to connect to")
		(options, args) = parser.parse_args(args=args)
		if len(args) > 0:
			parser.error("Invalid argument")
		clear(options.host)
	elif cmd in ['rls_clear']:
		parser = OptionParser("Usage: %prog rls_clear")
		(options, args) = parser.parse_args(args=args)
		if len(args) > 0:
			parser.error("Invalid argument")
		rls_clear()
	elif cmd in ['rls_direct_clear']:
		parser = OptionParser("Usage: %prog rls_direct_clear RLSHOST")
		(options, args) = parser.parse_args(args=args)
		if len(args) != 1:
			parser.error("Specify RLSHOST")
		rls_host = args[0]
		rls_direct_clear(rls_host)
	elif cmd in ['-h','help','-help','--help']:
		usage()
	else:
		sys.stderr.write("Unrecognized argument: %s\n" % cmd)
	
	
if __name__ == '__main__':
	main()
