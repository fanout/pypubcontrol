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
from ssl import SSLError
from .utilities import _gen_auth_jwt_header, _ensure_unicode

try:
	from http.client import IncompleteRead
except:
	from httplib import IncompleteRead

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
		self._get_subscribers_thread_result = False
		self._get_subscribers_thread = None
		self._thread_event = threading.Event()
		self._stream_thread = threading.Thread(target=self._run_stream)
		self._stream_thread.daemon = True
		self._stream_thread.start()

	def is_channel_subscribed_to(self, channel):
		found_channel = False
		self._lock.acquire()
		if channel in self._channels:
			found_channel = True
		self._lock.release()
		return found_channel

	def close(self):
		self._closed = True

	def is_failed(self):
		return self._failed

	def _run_stream(self):
		while not self._closed:
			print('trying to open stream')
			self._lock.acquire()
			self._channels = []
			self._lock.release()
			self._last_cursor = None
			wait_interval = 0
			retry_connection = True
			while retry_connection:
				time.sleep(wait_interval)
				wait_interval = PubSubMonitor._increase_wait_interval(wait_interval)
				try:
					if sys.version_info >= (2, 7, 9) or (ndg and ndg.httpsclient):
						self._stream_response = self._requests_session.get(
								self._stream_uri, headers=self._headers, stream=True,
								timeout=5)
					else:
						self._stream_response = self._requests_session.get(
								self._stream_uri, headers=self._headers, verify=False,
								stream=True, timeout=5)
					# No concern about a race condition here since there's 5 full
					# seconds between the .get() method above returning and the
					# timeout exception being thrown. The lines below are guaranteed
					# to execute within 5 seconds.
					if (self._stream_response.status_code >= 200 and
							self._stream_response.status_code < 300):
						retry_connection = False
					elif (self._stream_response.status_code < 500 or
							self._stream_response.status_code == 501 or
							self._stream_response.status_code >= 600):
						self._failed = True
						return
					else:
						continue
					print('opened stream')
					self._try_get_subscribers()
				except (socket.timeout, requests.exceptions.RequestException):
					continue
				got_subscribers = False
				while not self._closed:
					try:
						self._thread_event.wait()
						if not got_subscribers and self._get_subscribers_thread_result:
							got_subscribers = True
						if not got_subscribers:
							break
						self._monitor()
						break
					except (socket.timeout, requests.exceptions.Timeout, IncompleteRead):
						continue
					except (SSLError, OSError) as e:
						if 'timed out' in str(e):
							continue
						raise
				print('closing stream response')
				self._stream_response.close()
		print('pubsubmonitor run thread ended')

	def _monitor(self):
		print('monitoring stream')
		for line in self._stream_response.iter_lines(chunk_size=1):
			if line:
				content = json.loads(line)
				if self._catch_stream_up_to_last_cursor:
					if content['prev_cursor'] != self._last_cursor:
						continue
					self._catch_stream_up_to_last_cursor = False
				if content['prev_cursor'] != self._last_cursor:
					print('mismatch')
					got_subscribers = False
					self._try_get_subscribers()
					while not self._closed:
						try:
							self._thread_event.wait()
							got_subscribers = self._get_subscribers_thread_result
							break
						except (socket.timeout, requests.exceptions.Timeout):
							continue
					if not got_subscribers:
						break
				else:
					self._parse_items([content['item']])
				self._last_cursor = content['cursor']

	def _try_get_subscribers(self):
		self._thread_event.clear()
		self._get_subscribers_thread_result = False
		self._get_subscribers_thread = threading.Thread(target=self._run_get_subscribers)
		self._get_subscribers_thread.daemon = True
		self._get_subscribers_thread.start()

	def _run_get_subscribers(self):
		try:
			print('trying to get subscriber item list')
			items = []
			more_items_available = True
			while more_items_available:
				uri = self._items_uri
				if self._last_cursor:
					try:
						uri += "?" + urllib.urlencode({'since': 'cursor:' + self._last_cursor})
					except AttributeError:
						uri += "?" + urllib.parse.urlencode({'since': 'cursor:' + self._last_cursor})
				wait_interval = 0
				retry_connection = True
				while retry_connection:
					if wait_interval == 64:
						self._get_subscribers_thread_result = False
						return
					time.sleep(wait_interval)
					wait_interval = PubSubMonitor._increase_wait_interval(wait_interval)
					try:
						if sys.version_info >= (2, 7, 9) or (ndg and ndg.httpsclient):
							res = self._requests_session.get(uri, headers=self._headers,
									timeout=30)
						else:
							res = self._requests_session.get(uri, headers=self._headers,
									verify=False, timeout=30)
						if (res.status_code >= 200 and
								res.status_code < 300):
							retry_connection = False
						elif (res.status_code < 500 or
								res.status_code == 501 or
								res.status_code >= 600):
							self._get_subscribers_thread_result = False
							return
					except (socket.timeout, requests.exceptions.RequestException):
						pass
				content = json.loads(_ensure_unicode(res.content))
				self._last_cursor = content['last_cursor']
				if not content['items']:
					more_items_available = False
				else:
					items.extend(content['items'])
			print('got subscriber items list')
			self._parse_items(items)
			self._get_subscribers_thread_result = True
			self._catch_stream_up_to_last_cursor = True
		finally:
			self._thread_event.set()

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
