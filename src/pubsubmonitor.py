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
import time
import socket
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

# TODO: Kill thread when PubControlClient is closed.
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
		self._disabled = False
		self._thread = threading.Thread(target=self._start_monitoring)
		self._thread.daemon = True
		self._thread.start()

	def is_channel_subscribed_to(self, channel):
		if channel in self._channels:
			return True
		return False

	def _start_monitoring(self):
		continue_monitoring = True
		while continue_monitoring:
			self._channels = []
			wait_interval = 0
			retry_connection = True
			while retry_connection:
				time.sleep(wait_interval)
				wait_interval = PubSubMonitor._increase_wait_interval(wait_interval)
				try:
					if sys.version_info >= (2, 7, 9) or (ndg and ndg.httpsclient):
						self._stream_response = self._requests_session.get(self._stream_uri,
								headers=self._headers, stream=True)
					else:
						self._stream_response = self._requests_session.get(self._stream_uri,
								headers=self._headers, verify=False, stream=True)
					if (self._stream_response.status_code >= 200 and
							self._stream_response.status_code < 300):
						retry_connection = False
					elif (self._stream_response.status_code < 500 or
							self._stream_response.status_code == 501 or
							self._stream_response.status_code >= 600):
						self._disabled = True
						return
				except (socket.timeout, requests.exceptions.ConnectionError):
					pass
			cursor = self._get_subscribers()
			if cursor:
				self._monitor(cursor)

	def _monitor(self, cursor):
		# TODO: Implement sequencing.
		initial_sync = False
		for line in self._stream_response.iter_lines(chunk_size=1):
			if line:
				content = json.loads(line)
				if not initial_sync:
					print cursor + " : " + content['prev_cursor']
					if content['prev_cursor'] == cursor:
						initial_sync = True
						print 'stream in sync'
					else:
						print 'skipping record'
						continue
				else:
					if content['prev_cursor'] != cursor:
						break
				print content
				self._parse_items([content['item']])
				cursor = content['prev_cursor']
		print('finished monitor')

	def _get_subscribers(self):
		items = []
		last_cursor = ''
		continue_retrieval = True
		while continue_retrieval:
			uri = self._items_uri
			if last_cursor:
				 uri += "?" + urllib.urlencode({'since': 'cursor:' + last_cursor})
			wait_interval = 0
			retry_connection = True
			while retry_connection:
				if wait_interval == 64:
					return None
				time.sleep(wait_interval)
				wait_interval = PubSubMonitor._increase_wait_interval(wait_interval)
				try:
					if sys.version_info >= (2, 7, 9) or (ndg and ndg.httpsclient):
						res = self._requests_session.get(uri, headers=self._headers)
					else:
						res = self._requests_session.get(uri, headers=self._headers, verify=False)
					if (res.status_code >= 200 and
							res.status_code < 300):
						retry_connection = False
					elif (res.status_code < 500 or
							res.status_code == 501 or
							res.status_code >= 600):
						print res.status_code
						return None
				except (socket.timeout, requests.exceptions.ConnectionError):
					pass
			content = json.loads(res.content)
			last_cursor = content['last_cursor']
			print last_cursor
			if not content['items']:
				continue_retrieval = False
			else:
				items.extend(content['items'])
		self._parse_items(items)
		return last_cursor

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

	@staticmethod
	def _increase_wait_interval(wait_interval):
		if wait_interval <= 1:
			return wait_interval + 1
		elif wait_interval == 64:
			return wait_interval
		return wait_interval * 2
