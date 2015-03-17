#    pubcontrolclient.py
#    ~~~~~~~~~
#    This module implements the PubControlClient class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

from datetime import datetime
import calendar
import copy
import json
from base64 import b64encode
import threading
from collections import deque
import jwt

try:
	import urllib.request as urllib2
except ImportError:
	import urllib2

class PubControlClient(object):
	def __init__(self, uri):
		self.uri = uri
		self.lock = threading.Lock()
		self.thread = None
		self.thread_cond = None
		self.req_queue = deque()
		self.auth_basic_user = None
		self.auth_basic_pass = None
		self.auth_jwt_claim = None
		self.auth_jwt_key = None

	def set_auth_basic(self, username, password):
		self.lock.acquire()
		self.auth_basic_user = username
		self.auth_basic_pass = password
		self.lock.release()

	def set_auth_jwt(self, claim, key):
		self.lock.acquire()
		self.auth_jwt_claim = claim
		self.auth_jwt_key = key
		self.lock.release()

	# callback: func(boolean success, string message)
	# note: callback occurs in separate thread
	def publish(self, channel, item, blocking=False, callback=None):
		i = item.export()
		i['channel'] = channel
		if blocking:
			self.lock.acquire()
			uri = self.uri
			auth = self._gen_auth_header()
			self.lock.release()
			PubControlClient._pubcall(uri, auth, [i])
		else:
			self.lock.acquire()
			uri = self.uri
			auth = self._gen_auth_header()
			self._ensure_thread()
			self.lock.release()
			self._queue_req(('pub', uri, auth, i, callback))

	def finish(self):
		self.lock.acquire()
		if self.thread is not None:
			self._queue_req(('stop',))
			self.thread.join()
			self.thread = None
		self.lock.release()

	def _gen_auth_header(self):
		if self.auth_basic_user:
			return 'Basic ' + b64encode('%s:%s' % (self.auth_basic_user, self.auth_basic_pass))
		elif self.auth_jwt_claim:
			if 'exp' not in self.auth_jwt_claim:
				claim = copy.copy(self.auth_jwt_claim)
				claim['exp'] = calendar.timegm(datetime.utcnow().utctimetuple()) + 3600
			else:
				claim = self.auth_jwt_claim
			return 'Bearer ' + jwt.encode(claim, self.auth_jwt_key).decode('utf-8')
		else:
			return None

	def _ensure_thread(self):
		if self.thread is None:
			self.thread_cond = threading.Condition()
			self.thread = threading.Thread(target=self._pubworker)
			self.thread.daemon = True
			self.thread.start()

	def _queue_req(self, req):
		self.thread_cond.acquire()
		self.req_queue.append(req)
		self.thread_cond.notify()
		self.thread_cond.release()

	@staticmethod
	def _pubcall(uri, auth_header, items):
		uri = uri + '/publish/'

		headers = dict()
		if auth_header:
			headers['Authorization'] = auth_header
		headers['Content-Type'] = 'application/json'

		content = dict()
		content['items'] = items
		content_raw = json.dumps(content)

		try:
			if isinstance(content_raw, unicode):
				content_raw = content_raw.encode('utf-8')
		except NameError:
			if isinstance(content_raw, str):
				content_raw = content_raw.encode('utf-8')
			
		try:
			urllib2.urlopen(urllib2.Request(uri, content_raw, headers))
		except Exception as e:
			raise ValueError('failed to publish: ' + str(e))

	# reqs: list of (uri, auth_header, item, callback)
	@staticmethod
	def _pubbatch(reqs):
		assert(len(reqs) > 0)
		uri = reqs[0][0]
		auth_header = reqs[0][1]
		items = list()
		callbacks = list()
		for req in reqs:
			items.append(req[2])
			callbacks.append(req[3])

		try:
			PubControlClient._pubcall(uri, auth_header, items)
			result = (True, '')
		except Exception as e:
			try:
				result = (False, e.message)
			except AttributeError:
				result = (False, str(e))

		for c in callbacks:
			if c:
				c(result[0], result[1])

	def _pubworker(self):
		quit = False
		while not quit:
			self.thread_cond.acquire()

			# if no requests ready, wait for one
			if len(self.req_queue) == 0:
				self.thread_cond.wait()

				# still no requests after notification? start over
				if len(self.req_queue) == 0:
					self.thread_cond.release()
					continue

			reqs = list()
			while len(self.req_queue) > 0 and len(reqs) < 10:
				m = self.req_queue.popleft()
				if m[0] == 'stop':
					quit = True
					break
				reqs.append((m[1], m[2], m[3], m[4]))

			self.thread_cond.release()

			if len(reqs) > 0:
				PubControlClient._pubbatch(reqs)
