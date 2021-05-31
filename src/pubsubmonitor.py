#    pubsubmonitor.py
#    ~~~~~~~~~
#    This module implements the PubSubMonitor class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import requests
import threading
import json
import copy
import urllib
import time
import socket
import logging
from base64 import b64decode
from ssl import SSLError
from .utilities import _gen_auth_jwt_header, _ensure_unicode
from requests.exceptions import ConnectionError

logger = logging.getLogger(__name__)

try:
	from http.client import IncompleteRead
except:
	from httplib import IncompleteRead

# The PubSubMonitor class monitors subscriptions to channels via an HTTP interface.
class PubSubMonitor(object):

	# Initialize with base stream URI, JWT auth info, and callback used for indicating
	# when a subscription event occurs.
	def __init__(self, base_stream_uri, auth_jwt_claim=None, auth_jwt_key=None, callback=None):
		if base_stream_uri[-1:] != '/':
			base_stream_uri += '/'
		self._stream_uri = base_stream_uri + 'subscriptions/stream/'
		self._items_uri = base_stream_uri + 'subscriptions/items/'
		self._auth_jwt_claim = None
		if auth_jwt_claim:
			self._auth_jwt_claim = copy.deepcopy(auth_jwt_claim)
			self._auth_jwt_key = auth_jwt_key
		self._callback = callback
		self._lock = threading.Lock()
		self._requests_session = requests.session()
		self._stream_response = None
		self._channels = set()
		self._last_cursor = None
		self._closed = False
		self._historical_fetch_thread_result = False
		self._historical_fetch_thread = None
		self._thread_event = threading.Event()
		self._stream_thread = threading.Thread(target=self._run_stream)
		self._stream_thread.daemon = True
		self._stream_thread.start()

	# Determine if the specified channel has been subscribed to.
	def is_channel_subscribed_to(self, channel):
		found_channel = False
		self._lock.acquire()
		if channel in self._channels:
			found_channel = True
		self._lock.release()
		return found_channel

	# Close this instance and block until all threads are complete.
	def close(self, blocking=False):
		self._lock.acquire()
		self._closed = True
		self._callback = None
		stream_thread = self._stream_thread
		self._stream_thread = None
		self._lock.release()
		if blocking and stream_thread:
			stream_thread.join()

	# Determine if this instance is closed.
	def is_closed(self):
		return self._closed

	# Run the stream connection.
	def _run_stream(self):
		logger.debug('stream thread started')
		while not self._closed:
			wait_interval = 0
			retry_connection = True
			while retry_connection:
				time.sleep(wait_interval)
				wait_interval = PubSubMonitor._increase_wait_interval(wait_interval)
				try:
					logger.debug('stream get %s' % self._stream_uri)
					timeout = (5,60)
					headers = {}
					headers['Authorization'] = _gen_auth_jwt_header(
							self._auth_jwt_claim, self._auth_jwt_key)
					self._stream_response = self._requests_session.get(
							self._stream_uri, headers=headers, stream=True,
							timeout=timeout)
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
						self.close()
						raise ValueError(
								'pubsubmonitor stream connection resulted in status code: %d' %
								self._stream_response.status_code)
					else:
						continue
					logger.debug('stream open')
					self._try_historical_fetch()
				except (socket.timeout, requests.exceptions.RequestException):
					continue
				got_subscribers = False
				while not self._closed:
					try:
						self._thread_event.wait()
						if not got_subscribers and self._historical_fetch_thread_result:
							got_subscribers = True
						if not got_subscribers:
							break
						self._monitor()
						break
					except (socket.timeout, requests.exceptions.Timeout, IncompleteRead):
						logger.debug('stream timed out')
						break
					except (SSLError, OSError, ConnectionError) as e:
						if 'timed out' in str(e):
							logger.debug('stream timed out')
							break
						self._callback = None
						raise
					except:
						logger.exception('error processing stream')
				self._stream_response.close()
		logger.debug('stream thread ended')

	# Monitor the stream connection.
	def _monitor(self):
		for line in self._stream_response.iter_lines(chunk_size=1):
			if self._closed:
				break

			now = time.time()
			if (self._catch_stream_up_to_last_cursor and
					now >= self._catch_stream_up_start_time + 60):
				logger.debug('timed out waiting to catch up')
				break

			if not line:
				continue

			content = json.loads(_ensure_unicode(line))

			last_cursor_parsed = PubSubMonitor._parse_cursor(
					self._last_cursor)

			prev_cursor_parsed = None
			if 'prev_cursor' in content:
				prev_cursor_parsed = PubSubMonitor._parse_cursor(
						content['prev_cursor'])

			if self._catch_stream_up_to_last_cursor:
				if (prev_cursor_parsed and
						prev_cursor_parsed != last_cursor_parsed):
					continue
				logger.debug('stream caught up to last cursor')
				self._catch_stream_up_to_last_cursor = False
			if (prev_cursor_parsed and
					prev_cursor_parsed != last_cursor_parsed):
				logger.debug('stream cursor mismatch: got=%s expected=%s' % (
						prev_cursor_parsed, last_cursor_parsed))
				self._try_historical_fetch()
				self._thread_event.wait()
				if not self._historical_fetch_thread_result:
					break
			else:
				self._parse_items([content['item']])

			self._last_cursor = content['cursor']
			logger.debug('last cursor: %s' %
					PubSubMonitor._parse_cursor(self._last_cursor))

	# Try to complete the historical fetch.
	def _try_historical_fetch(self):
		self._thread_event.clear()
		self._historical_fetch_thread_result = False
		self._historical_fetch_thread = threading.Thread(target=self._run_historical_fetch)
		self._historical_fetch_thread.daemon = True
		self._historical_fetch_thread.start()

	# Run the historical fetch.
	def _run_historical_fetch(self):
		try:
			self._last_stream_cursor = None
			logger.debug('catching up')
			items = []
			more_items_available = True
			while more_items_available:
				uri = self._items_uri
				if self._last_cursor:
					try:
						uri += "?" + urllib.urlencode({'since': 'cursor:%s' % self._last_cursor})
					except AttributeError:
						uri += "?" + urllib.parse.urlencode({'since': 'cursor:%s' % self._last_cursor})
				wait_interval = 0
				retry_connection = True
				while retry_connection:
					if wait_interval == 64:
						self._historical_fetch_thread_result = False
						return
					time.sleep(wait_interval)
					wait_interval = PubSubMonitor._increase_wait_interval(wait_interval)
					try:
						if self._last_cursor:
							logger.debug('history get %s (%s)' % (
									uri, PubSubMonitor._parse_cursor(
									self._last_cursor)))
						else:
							logger.debug('history get %s' % uri)
						headers = {}
						headers['Authorization'] = _gen_auth_jwt_header(
								self._auth_jwt_claim, self._auth_jwt_key)
						res = self._requests_session.get(uri, headers=headers,
								timeout=30)
						if (res.status_code >= 200 and
								res.status_code < 300):
							retry_connection = False
						elif res.status_code == 404:
							self._unsub_and_clear_channels()
							self._historical_fetch_thread_result = False
							return
						elif (res.status_code < 500 or
								res.status_code == 501 or
								res.status_code >= 600):
							self._historical_fetch_thread_result = False
							self.close()
							raise ValueError(
									'pubsubmonitor historical fetch connection resulted in status code: %d' %
									res.status_code)
					except (socket.timeout, requests.exceptions.RequestException):
						pass
				content = json.loads(_ensure_unicode(res.content))
				self._last_cursor = content['last_cursor']
				if not content['items']:
					more_items_available = False
				else:
					items.extend(content['items'])
			self._parse_items(items)
			self._historical_fetch_thread_result = True
			self._catch_stream_up_to_last_cursor = True
			self._catch_stream_up_start_time = time.time()
			logger.debug('last cursor: %s' % PubSubMonitor._parse_cursor(self._last_cursor))
		finally:
			self._thread_event.set()

	# Unsubscribe from and clear all channels.
	def _unsub_and_clear_channels(self):
		logger.debug('unsubbing and clearing channels')
		if self._callback:
			for channel in self._channels:
				try:
					self._callback('unsub', channel)
				except Exception:
					logger.exception('error calling callback')
		self._lock.acquire()
		self._channels.clear()
		self._last_cursor = None
		self._lock.release()

	# Parse the specified items by updating the internal list and calling callbacks.
	def _parse_items(self, items):
		for item in items:
			if (item['state'] == 'subscribed' and
					item['channel'] not in self._channels):
				logger.debug('added %s' % item['channel'])
				if self._callback:
					try:
						self._callback('sub', item['channel'])
					except Exception:
						logger.exception('error calling callback')
				self._lock.acquire()
				self._channels.add(item['channel'])
				self._lock.release()
			elif (item['state'] == 'unsubscribed' and
					item['channel'] in self._channels):
				logger.debug('removed %s' % item['channel'])
				if self._callback:
					try:
						self._callback('unsub', item['channel'])
					except Exception:
						logger.exception('error calling callback')
				self._lock.acquire()
				self._channels.remove(item['channel'])
				self._lock.release()

	# Parse the specified cursor.
	@staticmethod
	def _parse_cursor(raw_cursor):
		decoded_cursor = b64decode(raw_cursor).decode('UTF-8')
		return decoded_cursor[decoded_cursor.index('_')+1:]

	# Increase the wait interval from the specified interval.
	@staticmethod
	def _increase_wait_interval(wait_interval):
		if wait_interval <= 1:
			return wait_interval + 1
		elif wait_interval == 64:
			return wait_interval
		return wait_interval * 2
