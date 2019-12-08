#    pubcontrolclient.py
#    ~~~~~~~~~
#    This module implements the PubControlClient class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import json
import copy
from base64 import b64encode
import threading
from collections import deque
import requests
from requests.packages.urllib3.util import Retry
from requests.adapters import HTTPAdapter
from .pubsubmonitor import PubSubMonitor
from .utilities import _gen_auth_jwt_header

# The PubControlClient class allows consumers to publish either synchronously
# or asynchronously to an endpoint of their choice. The consumer wraps a Format
# class instance in an Item class instance and passes that to the publish
# method. The async publish method has an optional callback parameter that
# is called after the publishing is complete to notify the consumer of the
# result. Optionally provide JWT authentication claim and key information.
# If require_subscribers is set to True then channel subscription monitoring
# will be enabled and only channels that are subscribed to will be published
# to.
class PubControlClient(object):

	# Initialize this class with a URL representing the publishing endpoint.
	def __init__(self, uri, auth_jwt_claim=None,
			auth_jwt_key=None, require_subscribers=False, sub_callback=None):
		self.uri = uri
		self.lock = threading.Lock()
		self.thread = None
		self.thread_cond = None
		self.req_queue = deque()
		self.auth_basic_user = None
		self.auth_basic_pass = None
		self.auth_jwt_claim = auth_jwt_claim
		self.auth_jwt_key = auth_jwt_key
		self.sub_monitor = None
		self.closed = False

		retry = Retry(
			total=1,
			backoff_factor=0.5,
			status_forcelist=[500, 502, 503, 504],
			method_whitelist=frozenset(['GET', 'POST'])
		)

		adapter = HTTPAdapter(max_retries=retry)

		self.requests_session = requests.session()
		self.requests_session.mount('http://', adapter)
		self.requests_session.mount('https://', adapter)

		if require_subscribers:
			self.sub_monitor = PubSubMonitor(uri, auth_jwt_claim, auth_jwt_key, sub_callback)

	# Call this method and pass a username and password to use basic
	# authentication with the configured endpoint.
	def set_auth_basic(self, username, password):
		self._verify_notclosed()
		self.lock.acquire()
		self.auth_basic_user = username
		self.auth_basic_pass = password
		self.lock.release()

	# Call this method and pass a claim and key to use JWT authentication
	# with the configured endpoint.
	def set_auth_jwt(self, claim, key):
		self._verify_notclosed()
		self.lock.acquire()
		self.auth_jwt_claim = claim
		self.auth_jwt_key = key
		self.lock.release()

	# The publish method for publishing the specified item to the specified
	# channel on the configured endpoint. The blocking parameter indicates
	# whether the the callback method should be blocking or non-blocking. The
	# callback parameter is optional and will be passed the publishing results
	# after publishing is complete. Note that the callback executes on a
	# separate thread. If require_subscribers was set to True then the message
	# will only be published if the channel is subscribed to. If the sub_monitor
	# instance failed to retrieve subscriber information then an error will be
	# raised.
	def publish(self, channel, item, blocking=False, callback=None):
		self._verify_notclosed()
		if self.sub_monitor and self.sub_monitor.is_closed():
			if callback:
				callback(False, 'failed to retrieve channel subscribers')
			else:
				raise ValueError('failed to retrieve channel subscribers')
		elif self.sub_monitor and not self.sub_monitor.is_channel_subscribed_to(channel):
			if callback:
				callback(True, '')
			return
		i = item.export()
		i['channel'] = channel
		if blocking:
			self.lock.acquire()
			uri = self.uri
			auth = self._gen_auth_header()
			self.lock.release()
			self._pubcall(uri, auth, [i])
		else:
			self.lock.acquire()
			uri = self.uri
			auth = self._gen_auth_header()
			self._ensure_thread()
			self.lock.release()
			self._queue_req(('pub', uri, auth, i, callback))

	# This method is a blocking method that ensures that all asynchronous
	# publishing is complete prior to returning and allowing the consumer to
	# proceed.
	def wait_all_sent(self):
		self.lock.acquire()
		if self.thread is not None:
			self._queue_req(('stop',))
			self.thread.join()
			self.thread = None
		self.lock.release()

	# DEPRECATED: The finish method is now deprecated in favor of the more
	# descriptive wait_all_sent() method.
	def finish(self):
		self.wait_all_sent()

	# This method is meant to close the PubControlClient instance and should
	# implemented when needed for future features. Currently this method
	# is simply a passthrough to wait_all_sent().
	def close(self):
		self.lock.acquire()
		self.closed = True
		if self.sub_monitor:
			self.sub_monitor.close()
		self.lock.release()
		self.wait_all_sent()

	# This method makes an HTTP request to an endpoint relative to the base
	# URI, using configured authentication. Returns a tuple of
	# (status code, headers, body).
	def http_call(self, endpoint, data, headers={}):
		uri = self.uri + endpoint

		send_headers = copy.deepcopy(headers)

		auth_header = self._gen_auth_header()
		if auth_header:
			send_headers['Authorization'] = auth_header

		try:
			return self._make_http_request(uri, data, send_headers)
		except Exception as e:
			raise ValueError('failed during http call: ' + str(e))

	# An internal method for verifying that the PubControlClient instance
	# has not been closed via the close() method. If it has then an error
	# is raised.
	def _verify_notclosed(self):
		if self.closed:
			raise ValueError('pubcontrolclient instance is closed')

	# An internal method used to generate an authorization header. The
	# authorization header is generated based on whether basic or JWT
	# authorization information was provided via the publicly accessible
	# 'set_*_auth' methods defined above.
	def _gen_auth_header(self):
		if self.auth_basic_user:
			return 'Basic ' + str(b64encode(('%s:%s' % (self.auth_basic_user, self.auth_basic_pass)).encode('ascii')))
		elif self.auth_jwt_claim:
			return _gen_auth_jwt_header(self.auth_jwt_claim, self.auth_jwt_key)
		else:
			return None

	# An internal method that ensures that asynchronous publish calls are
	# properly processed. This method initializes the required class fields,
	# starts the pubworker worker thread, and is meant to execute only when
	# the consumer makes an asynchronous publish call.
	def _ensure_thread(self):
		if self.thread is None:
			self.thread_cond = threading.Condition()
			self.thread = threading.Thread(target=self._pubworker)
			self.thread.daemon = True
			self.thread.start()

	# An internal method for adding an asynchronous publish request to the
	# publishing queue. This method will also activate the pubworker worker
	# thread to make sure that it process any and all requests added to
	# the queue.
	def _queue_req(self, req):
		self.thread_cond.acquire()
		self.req_queue.append(req)
		self.thread_cond.notify()
		self.thread_cond.release()

	# An internal method for preparing the HTTP POST request for publishing
	# data to the endpoint. This method accepts the URI endpoint, authorization
	# header, and a list of items to publish.
	def _pubcall(self, uri, auth_header, items):
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
			self._make_http_request(uri, content_raw, headers)
		except Exception as e:
			raise ValueError('failed to publish: ' + str(e))

	# An internal method for making an HTTP request to the specified URI
	# with the specified content and headers.
	def _make_http_request(self, uri, data, headers):
		res = self.requests_session.post(uri, headers=headers, data=data)
		self._verify_status_code(res.status_code, res.text)
		return (res.status_code, res.headers, res.text)

	# An internal method for ensuring a successful status code is returned
	# from the server.
	def _verify_status_code(self, code, message):
		if code < 200 or code >= 300:
			raise ValueError('received failed status code ' + str(code) +
					' with message: ' + message)

	# An internal method for publishing a batch of requests. The requests are
	# parsed for the URI, authorization header, and each request is published
	# to the endpoint. After all publishing is complete, each callback
	# corresponding to each request is called (if a callback was originally
	# provided for that request) and passed a result indicating whether that
	# request was successfully published.
	def _pubbatch(self, reqs):
		assert(len(reqs) > 0)
		uri = reqs[0][0]
		auth_header = reqs[0][1]
		items = list()
		callbacks = list()
		for req in reqs:
			items.append(req[2])
			callbacks.append(req[3])

		try:
			self._pubcall(uri, auth_header, items)
			result = (True, '')
		except Exception as e:
			try:
				result = (False, e.message)
			except AttributeError:
				result = (False, str(e))

		for c in callbacks:
			if c:
				c(result[0], result[1])

	# An internal method that is meant to run as a separate thread and process
	# asynchronous publishing requests. The method runs continously and
	# publishes requests in batches containing a maximum of 10 requests. The
	# method completes and the thread is terminated only when a 'stop' command
	# is provided in the request queue.
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
				self._pubbatch(reqs)
