#    pubsubmonitor.py
#    ~~~~~~~~~
#    This module implements the PubSubMonitor class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import sys
import requests
import threading
from .utilities import _gen_auth_jwt_header

try:
	import ndg.httpsclient
except ImportError:
	ndg = None

# TODO: Remove in the future when most users have Python >= 2.7.9.
try:
	import urllib3
except ImportError:
	try:
		from requests.packages import urllib3
	except ImportError:
		pass
try:
	urllib3.disable_warnings()
except NameError:
	pass
except AttributeError:
	pass

class PubSubMonitor(object):
	def __init__(self, base_uri, auth_jwt_claim=None, auth_jwt_key=None):
		if base_uri[-1:] != '/':
			base_uri += '/'
		self._uri = base_uri + 'subscriptions/stream/'
		if auth_jwt_claim:
			self._headers = dict()
			self._headers['Authorization'] = _gen_auth_jwt_header(
					auth_jwt_claim, auth_jwt_key)
		self._thread = threading.Thread(target=self._start_monitoring)
		self._thread.daemon = True
		self._thread.start()

	def _start_monitoring(self):
		self._requests_session = requests.session()
		if sys.version_info >= (2, 7, 9) or (ndg and ndg.httpsclient):
			res = self._requests_session.get(self._uri, headers=self._headers,
					stream=True)
		else:
			res = self._requests_session.get(self._uri, headers=self._headers,
					verify=False, stream=True)
		for line in res.iter_lines(chunk_size=1):
			print line
			if line:
				print(line)
		print 'finished'
