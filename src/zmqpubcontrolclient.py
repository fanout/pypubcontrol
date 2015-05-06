#    zmqpubcontrolclient.py
#    ~~~~~~~~~
#    This module implements the ZmqPubControlClient class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import threading
import atexit
from .utilities import _ensure_utf8
from .zmqpubcontroller import ZmqPubController

try:
	import zmq
except ImportError:
	zmq = None

try:
	import tnetstring
except ImportError:
	tnetstring = None

# The global list of ZmqPubControlClient instances used to ensure that each
# instance is properly closed on exit.
_zmqpubcontrolclients = list()
_lock = threading.Lock()

# An internal method used for closing all existing ZmqPubControlClient instances.
def _close_zmqpubcontrolclients():
	_lock.acquire()
	zmqpubcontrolclients = list(_zmqpubcontrolclients)
	for zmqpubcontrolclient in zmqpubcontrolclients:
		zmqpubcontrolclient.close()
	_lock.release()

# Register the _close_zmqpubcontrolclients method with atexit to ensure that
# it is called on exit.
atexit.register(_close_zmqpubcontrolclients)

# The ZmqPubControlClient class allows consumers to publish to a ZMQ endpoint
# of their choice. The consumer wraps a Format instance in an Item instance
# and passes it to the publish method. Note that a ZmqPubControlClient instance
# that has been closed via the 'close' method will raise an exception if it
# is used.
class ZmqPubControlClient(object):

	# Initialize this class with a URL representing the REQ socket endpoint.
	# Optionally provide ZMQ PUB and PUSH URI endpoints and a boolean indicating
	# if publishes should only occur when subscribers are available. If the
	# disable_pub boolean is set then initializing with a PUB socket URI or
	# attempting to publish on a PUB socket will result in an exception (this
	# is done to facilitate PUB socket publishing from the PubControl class).
	# Optionally specify a ZMQ context to use otherwise the global ZMQ context
	# will be used.
	def __init__(self, uri, zmq_push_uri=None, zmq_pub_uri=None,
			require_subscribers=False, disable_pub=False, sub_callback=None, 
			zmq_context=None):
		if zmq is None:
			raise ValueError('zmq package must be installed')
		if tnetstring is None:
			raise ValueError('tnetstring package must be installed')
		self.uri = uri
		self.zmq_pub_uri = zmq_pub_uri
		self.zmq_push_uri = zmq_push_uri
		self.closed = False
		self._zmq_ctx = zmq_context
		if self._zmq_ctx is None:
			self._zmq_ctx = zmq.Context.instance()
		self._require_subscribers = require_subscribers
		self._disable_pub = disable_pub
		self._sub_callback = sub_callback
		self._lock = threading.Lock()
		self._push_sock = None
		self._zmq_pub_controller = None
		if self.zmq_push_uri or self.zmq_pub_uri:
			self.connect_zmq()
		_lock.acquire()
		_zmqpubcontrolclients.append(self)
		_lock.release()

	# The publish method for publishing the specified item to the specified
	# channel on the configured ZMQ endpoint. Note that ZMQ publishes are
	# always non-blocking regardless of whether the blocking parameter is set.
	# Also, if a callback is specified, the callback will always be called with
	# a result that is set to true.
	def publish(self, channel, item, blocking=False, callback=None):
		self._verify_not_closed()
		self.connect_zmq()
		if self._push_sock is None and self._zmq_pub_controller is None:
			if callback:
				callback(True, '')
			return
		i = item.export(True, True)
		channel = _ensure_utf8(channel)
		self._send_to_zmq(i, channel)
		if callback:
			callback(True, '')

	# The close method is a blocking call that closes all ZMQ sockets prior
	# to returning and allowing the consumer to proceed. Note that the
	# ZmqPubControlClient instance cannot be used after calling this method.
	def close(self):
		self._lock.acquire()
		self._verify_not_closed()
		if self._zmq_pub_controller:
			self._zmq_pub_controller.stop()
			self._zmq_pub_controller._thread.join()
			self._zmq_pub_controller = None
		if self._push_sock:
			self._push_sock.close()
			self._push_sock = None
		_zmqpubcontrolclients.remove(self)
		self.closed = True
		self._lock.release()

	# A thread-safe method for connecting to the configured ZMQ endpoints.
	# If a PUSH URI is configured and require_subscribers is set to false
	# then a PUSH socket will be created. If a PUB URI is configured and
	# disable_pub is false then a ZmqPubController instance along with a
	# corresponding control socket will be created.
	def connect_zmq(self):
		self._verify_not_closed()
		self._verify_uri_config()
		self._lock.acquire()
		if self._push_sock is None and self._zmq_pub_controller is None:
			if (self.zmq_pub_uri and not self._disable_pub and
					(self.zmq_push_uri is None or self._require_subscribers)):
				self._zmq_pub_controller = ZmqPubController(self._sub_callback,
						self._zmq_ctx)
				self._zmq_pub_controller.connect(self.zmq_pub_uri)
			elif (self.zmq_push_uri and not self._require_subscribers):
				self._push_sock = self._zmq_ctx.socket(zmq.PUSH)
				self._push_sock.connect(self.zmq_push_uri)
				self._push_sock.linger = 0
		self._lock.release()

	# An internal method for ensuring that the ZMQ URIs are properly set
	# relative to the require_subscribers and disable_pub booleans.
	def _verify_uri_config(self):
		if self.zmq_pub_uri is None and self.zmq_push_uri is None:
			raise ValueError('either a zmq pub or push uri must be set to publish')
		if self.zmq_pub_uri is None and self._require_subscribers:
			raise ValueError('zmq_pub_uri must be set if _require_subscribers ' +
					'is set to true')

	# An internal method for publishing a ZMQ message to either the ZMQ
	# push socket or ZmqPubController.
	def _send_to_zmq(self, content, channel):
		self._lock.acquire()
		if self._push_sock:
			content['channel'] = channel
			self._push_sock.send(tnetstring.dumps(content))
		else:
			self._zmq_pub_controller.publish(channel +
					'\x00' + tnetstring.dumps(content))
		self._lock.release()

	# An internal method for verifying that the ZmqPubControlClient instance
	# has not been closed via the close() method. If it has then an error
	# is raised.
	def _verify_not_closed(self):
		if self.closed:
			raise ValueError('zmqpubcontrolclient instance is closed')
