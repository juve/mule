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
import os
import sys
	
def daemonize():
	"""Turn this process into a daemon by detaching from our parent,
	closing all open file descriptors, and redirecting std[in,out,err]
	to the bit bucket."""
	
	# First fork
	pid = os.fork()
	if pid > 0: sys.exit(0)
	
	# Detach from parent
	os.setsid()
	os.umask(0)
	
	# Second fork
	pid = os.fork() 
	if pid > 0: sys.exit(0)

	# Close all file descriptors	
	import resource
	maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
	if (maxfd == resource.RLIM_INFINITY):
		maxfd = 1024
	for fd in range(0, maxfd):
		try: os.close(fd)
		except OSError: pass

	if (hasattr(os, "devnull")):
		REDIRECT_TO = os.devnull
	else:
		REDIRECT_TO = "/dev/null"
	
	os.open(REDIRECT_TO, os.O_RDWR)	# standard input (0)
	os.dup2(0, 1)					# standard output (1)
	os.dup2(0, 2)					# standard error (2)