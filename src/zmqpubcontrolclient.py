#    zmqpubcontrolclient.py
#    ~~~~~~~~~
#    This module implements the ZmqPubControlClient class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import threading
import atexit
from .utilities import _ensure_utf8, _verify_zmq
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
	# if publishes should only occur when subscribers are available. If a REQ
	# endpoint was specified then the PUB and PUSH URIs will automatically be
	# discovered unless they were already explicitly provided in the constructor.
	# If the disable_pub boolean is set then initializing with a PUB socket URI or
	# attempting to publish on a PUB socket will result in an exception (this
	# is done to facilitate PUB socket publishing from the PubControl class).
	# Optionally specify a ZMQ context to use otherwise the global ZMQ context
	# will be used.
	def __init__(self, uri, push_uri=None, pub_uri=None,
			require_subscribers=False, disable_pub=False, sub_callback=None, 
			context=None):
		_verify_zmq()
		self.uri = uri
		self.pub_uri = pub_uri
		self.push_uri = push_uri
		self._context = context
		if self._context is None:
			self._context = zmq.Context.instance()
		if self.uri:
			self._discover_uris()
		self.closed = False
		self._require_subscribers = require_subscribers
		self._disable_pub = disable_pub
		self._sub_callback = sub_callback
		self._lock = threading.Lock()
		self._push_sock = None
		self._pub_controller = None
		if self.push_uri or self.pub_uri:
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
		if self._push_sock is None and self._pub_controller is None:
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
		if self._pub_controller:
			self._pub_controller.stop()
			self._pub_controller._thread.join()
			self._pub_controller = None
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
		if self._push_sock is None and self._pub_controller is None:
			if not self._disable_pub and self._require_subscribers:
				self._pub_controller = ZmqPubController(self._sub_callback,
						self._context)
				self._pub_controller.connect(self.pub_uri)
			elif not self._require_subscribers:
				self._push_sock = self._context.socket(zmq.PUSH)
				self._push_sock.connect(self.push_uri)
				self._push_sock.linger = 0
		self._lock.release()

	# An internal method for ensuring that the ZMQ URIs are properly set
	# relative to the require_subscribers and disable_pub booleans.
	def _verify_uri_config(self):
		if self.pub_uri is None and self._require_subscribers:
			raise ValueError('pub_uri must be set if require_subscribers ' +
					'is set to true')
		if self.push_uri is None and not self._require_subscribers:
			raise ValueError('push_uri must be set if require_subscribers ' +
					'is set to false')
		if self._sub_callback and not self._require_subscribers:
			raise ValueError('sub_callback can only be specified when ' +
					' require_subscribers is set to true')

	# An internal method for publishing a ZMQ message to either the ZMQ
	# push socket or ZmqPubController.
	def _send_to_zmq(self, content, channel):
		self._lock.acquire()
		if self._push_sock:
			content['channel'] = channel
			self._push_sock.send(tnetstring.dumps(content))
		else:
			self._pub_controller.publish(channel,
					tnetstring.dumps(content))
		self._lock.release()

	# An internal method for verifying that the ZmqPubControlClient instance
	# has not been closed via the close() method. If it has then an error
	# is raised.
	def _verify_not_closed(self):
		if self.closed:
			raise ValueError('zmqpubcontrolclient instance is closed')

	# An internal method for discovering the ZMQ PUB and PUSH URIs when a
	# command URI is availabe. If either a PUB or a PUSH URI was already
	# specified by the consumer then those will be used.
	def _discover_uris(self):
		if self.pub_uri and self.push_uri:
			return
		command_uri = self.uri
		command_host = None
		if command_uri.startswith('tcp://'):
			at = command_uri.find(':', 6)
			command_host = command_uri[6:at]
		sock = self._context.socket(zmq.REQ)
		sock.connect(command_uri)
		req = {'method': 'get-zmq-uris'}
		sock.send(tnetstring.dumps(req))
		resp = tnetstring.loads(sock.recv())
		if not resp.get('success'):
			sock.close()
			raise ValueError('uri discovery request failed: %s' % resp)
		v = resp['value']
		if self.push_uri is None and 'publish-pull' in v:
			self.push_uri = self._resolve_uri(v['publish-pull'], command_host)
		if self.pub_uri is None and 'publish-sub' in v:
			self.pub_uri = self._resolve_uri(v['publish-sub'], command_host)
		sock.close()

	# An internal method for resolving a ZMQ URI when the URI contains an
	# asterisk representing all network interfaces.
	def _resolve_uri(self, uri, command_host):
		if uri.startswith('tcp://'):
			at = uri.find(':', 6)
			addr = uri[6:at]
			if addr == '*':
				if command_host:
					return uri[0:6] + command_host + uri[at:]
				else:
					return uri[0:6] + 'localhost' + uri[at:]
		return uri
