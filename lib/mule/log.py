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
import logging
import logging.handlers
from datetime import datetime
from mule import config

__all__ = ["get_log","DEBUG","INFO","WARNING","ERROR","CRITICAL"]
	
FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

DEFAULT_LEVEL = DEBUG

CONSOLE = None
FILE = None

def create_console_handler():
	formatter = logging.Formatter(FORMAT)
	handler = logging.StreamHandler(sys.stdout)
	handler.setLevel(DEFAULT_LEVEL)
	handler.setFormatter(formatter)
	return handler
	
def create_file_handler():
	exe = os.path.basename(sys.argv[0])
	if exe == "mule-agent":
		logfile = os.path.join(config.get_home(),"var","agent.log")
		formatter = logging.Formatter(FORMAT)
		handler = logging.handlers.RotatingFileHandler(logfile,maxBytes=100000,backupCount=1)
		handler.setLevel(DEFAULT_LEVEL)
		handler.setFormatter(formatter)
		return handler
	elif exe == "mule-rls":
		logfile = os.path.join(config.get_home(),"var","rls.log")
		formatter = logging.Formatter(FORMAT)
		handler = logging.handlers.RotatingFileHandler(logfile,maxBytes=100000,backupCount=1)
		handler.setLevel(DEFAULT_LEVEL)
		handler.setFormatter(formatter)
		return handler
	else:
		# Everything else has no log file, only console
		return None

def configure():
	"""Configure logging for this process"""
	global CONSOLE
	global FILE
	CONSOLE = create_console_handler()
	FILE = create_file_handler()

def get_log(name, level=DEBUG):
	"""Get a logger instance with the given name"""
	global CONSOLE
	global FILE
	logger = logging.getLogger(name)
	logger.setLevel(level)
	if CONSOLE is not None: logger.addHandler(CONSOLE)
	if FILE is not None: logger.addHandler(FILE)
	return logger

if __name__ == '__main__':
	configure()
	log = get_log("foo")
	log.debug("debug message")
	log.info("info message")
	log.warn("warning message")
	log.error("error message")
	try:
		raise Exception("exception message")
	except Exception, e:
		log.exception(e)
		
	log2 = get_log("bar")
	print log.handlers
	print log2.handlers