#    zmqpubcontrolclient.py
#    ~~~~~~~~~
#    This module implements the ZmqPubControlClient class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import threading
from .utilities import _ensure_utf8

try:
	import zmq
except ImportError:
	zmq = None

try:
	import tnetstring
except ImportError:
	tnetstring = None

# The ZmqPubControlClient class allows consumers to publish to a ZMQ endpoint
# of their choice. The consumer wraps a Format instance in an Item instance
# and passes it to the publish method.
class ZmqPubControlClient(object):

	# Initialize this class with a URL representing the REQ socket endpoint.
	# Optionally provide ZMQ PUB and PUSH URI endpoints and a boolean indicating
	# if publishes should only occur when subscribers are available. If the
	# disable_pub boolean is set then initializing with a PUB socket URI or
	# attempting to publish on a PUB socket will result in an exception (this
	# is done to facilitate PUB socket publishing from the PubControl class).
	def __init__(self, uri, zmq_push_uri=None, zmq_pub_uri=None,
			require_subscriptions=False, disable_pub=False):
		if zmq is None:
			raise ValueError('zmq package must be installed')
		if tnetstring is None:
			raise ValueError('tnetstring package must be installed')
		self.uri = uri
		self.zmq_pub_uri = zmq_pub_uri
		self.zmq_push_uri = zmq_push_uri
		self.require_subscriptions = require_subscriptions
		self.disable_pub = disable_pub
		self._lock = threading.Lock()
		self._zmq_ctx = None
		self._zmq_sock = None
		if self.zmq_push_uri is not None or self.zmq_pub_uri is not None:
			self._connect_zmq()

	# The publish method for publishing the specified item to the specified
	# channel on the configured ZMQ endpoint. Note that ZMQ publishes are
	# always non-blocking regardless of whether the blocking parameter is set.
	# Also, if a callback is specified, the callback will always be called with
	# a result that is set to true.
	def publish(self, channel, item, blocking=False, callback=None):
		self._connect_zmq()
		if self._zmq_sock is None:
			return
		i = item.export(True, True)
		channel = _ensure_utf8(channel)
		self._send_to_zmq(i, channel)
		if callback:
			callback(True, '')

	# Close the open ZMQ socket in this instance and set the ZMQ context and
	# socket to None.
	def close(self):
		self._lock.acquire()
		if self._zmq_sock is not None:
			self._zmq_sock.close()
			self._zmq_sock = None
			self._zmq_ctx = None
		self._lock.release()
	
	# An internal method for ensuring that the ZMQ URIs are properly set
	# relative to the require_subscribers and disable_pub booleans.
	def _verify_uri_config(self):
		if self.zmq_pub_uri is None and self.zmq_push_uri is None:
			raise ValueError('either a zmq pub or push uri must be set to publish')
		if self.zmq_pub_uri is None and self.require_subscriptions:
			raise ValueError('zmq_pub_uri must be set if require_subscriptions ' +
					'is set to true')

	# An internal method for setting up and connecting to the ZMQ endpoint
	# depending on the PUSH / PUB configuration.
	def _connect_zmq(self):
		self._verify_uri_config()
		self._lock.acquire()
		if self._zmq_ctx is None and self._zmq_sock is None:
			if (self.zmq_pub_uri is not None and self.disable_pub is False and
					(self.zmq_push_uri is None or self.require_subscriptions)):
				self._zmq_ctx = zmq.Context()
				self._zmq_sock = self._zmq_ctx.socket(zmq.XPUB)
				self._zmq_sock.connect(self.zmq_pub_uri)
				self._zmq_sock.linger = 0
				print 'created push socket in client'
			elif (self.zmq_push_uri is not None and
					self.require_subscriptions is False):
				self._zmq_ctx = zmq.Context()
				self._zmq_sock = self._zmq_ctx.socket(zmq.PUSH)
				self._zmq_sock.connect(self.zmq_push_uri)
				self._zmq_sock.linger = 0
				print 'created push socket in client'
		self._lock.release()
		
	# An internal method for publishing a ZMQ message to the configured ZMQ
	# socket and specified channel.
	def _send_to_zmq(self, content, channel):
		if self._zmq_sock.socket_type == zmq.PUSH:
			content['channel'] = channel
			self._zmq_sock.send(tnetstring.dumps(content))
			print 'client push publish'
		else:
			self._zmq_sock.send_multipart([channel, tnetstring.dumps(content)])
			print 'client pub publish'
