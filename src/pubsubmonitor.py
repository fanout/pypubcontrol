#    pubsubmonitor.py
#    ~~~~~~~~~
#    This module implements the PubSubMonitor class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import sys
import requests
import threading
import json
import urllib
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
	def __init__(self, base_stream_uri, auth_jwt_claim=None, auth_jwt_key=None):
		if base_stream_uri[-1:] != '/':
			base_stream_uri += '/'
		self._stream_uri = base_stream_uri + 'subscriptions/stream/'
		self._items_uri = base_stream_uri + 'subscriptions/items/'
		if auth_jwt_claim:
			self._headers = dict()
			self._headers['Authorization'] = _gen_auth_jwt_header(
					auth_jwt_claim, auth_jwt_key)
		self._requests_session = requests.session()
		self._channels = []
		self._thread = threading.Thread(target=self._start_monitoring)
		self._thread.daemon = True
		self._thread.start()

	def is_channel_subscribed_to(self, channel):
		if channel in self._channels:
			return True
		return False

	def _start_monitoring(self):
		if sys.version_info >= (2, 7, 9) or (ndg and ndg.httpsclient):
			self._stream_response = self._requests_session.get(self._stream_uri,
					headers=self._headers, stream=True)
		else:
			self._stream_response = self._requests_session.get(self._stream_uri,
					headers=self._headers, verify=False, stream=True)
		self._get_subscribers()
		self._monitor()

	def _monitor(self):
		for line in self._stream_response.iter_lines(chunk_size=1):
			content = json.loads(line)
			if 'item' in content:
				self._parse_items([content['item']])

	def _get_subscribers(self):
		items = []
		last_cursor = ''
		continue_retrieval = True
		while continue_retrieval:
			uri = self._items_uri
			if last_cursor:
				 uri += "?" + urllib.urlencode({'since': 'cursor:' + last_cursor})
			res = self._requests_session.get(uri, headers=self._headers)
			content = json.loads(res.content)
			last_cursor = content['last_cursor']
			if not content['items']:
				continue_retrieval = False
			else:
				items.extend(content['items'])
		self._parse_items(items)

	def _parse_items(self, items):
		for item in items:
			if (item['state'] == 'subscribed' and
					item['channel'] not in self._channels):
				self._channels.append(item['channel'])
				print('added ' + item['channel'])
			elif (item['state'] == 'unsubscribed' and
					item['channel'] in self._channels):
				self._channels.remove(item['channel'])
				print('removed ' + item['channel'])