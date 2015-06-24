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
		self._lock = threading.Lock()
		self._requests_session = requests.session()
		self._stream_response = None
		self._channels = []
		self._failed = False
		self._closed = False
		self._thread = threading.Thread(target=self._run)
		self._thread.daemon = True
		self._thread.start()

	def is_channel_subscribed_to(self, channel):
		found_channel = False
		self._lock.acquire()
		if channel in self._channels:
			found_channel = True
		self._lock.release()
		return found_channel

	def close(self):
		print 'closing stream response'
		self._closed = True
		if self._stream_response:
			self._stream_response.close()

	def is_failed(self):
		return self._failed

	def _run(self):
		while not self._closed:
			self._lock.acquire()
			self._channels = []
			self._lock.release()
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
						self._failed = True
						return
				except (socket.timeout, requests.exceptions.ConnectionError):
					pass
			if self. _try_get_subscribers():
				self._monitor()
		print 'pubsubmonitor thread ended'

	def _monitor(self):
		print 'monitoring stream'
		last_cursor = None
		for line in self._stream_response.iter_lines(chunk_size=1):
			if line:
				content = json.loads(line)
				if last_cursor and content['prev_cursor'] != last_cursor:
					print 'mismatch'
					if not self. _try_get_subscribers(last_cursor):
						break
				else:
					self._parse_items([content['item']])
				last_cursor = content['cursor']
		print 'no more stream lines to iterate'
		self._stream_response.close()

	def _try_get_subscribers(self, last_cursor=None):
		print 'getting subscribers'
		items = []
		continue_retrieval = True
		while continue_retrieval:
			uri = self._items_uri
			if last_cursor:
				 uri += "?" + urllib.urlencode({'since': 'cursor:' + last_cursor})
			wait_interval = 0
			retry_connection = True
			while retry_connection:
				if wait_interval == 64:
					return False
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
						return False
				except (socket.timeout, requests.exceptions.ConnectionError):
					pass
			content = json.loads(res.content)
			last_cursor = content['last_cursor']
			if not content['items']:
				continue_retrieval = False
			else:
				items.extend(content['items'])
		self._parse_items(items)
		return True

	def _parse_items(self, items):
		self._lock.acquire()
		for item in items:
			if (item['state'] == 'subscribed' and
					item['channel'] not in self._channels):
				self._channels.append(item['channel'])
				print('added ' + item['channel'])
			elif (item['state'] == 'unsubscribed' and
					item['channel'] in self._channels):
				self._channels.remove(item['channel'])
				print('removed ' + item['channel'])
		self._lock.release()

	@staticmethod
	def _increase_wait_interval(wait_interval):
		if wait_interval <= 1:
			return wait_interval + 1
		elif wait_interval == 64:
			return wait_interval
		return wait_interval * 2
