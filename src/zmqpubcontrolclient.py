#    zmqpubcontrolclient.py
#    ~~~~~~~~~
#    This module implements the ZmqPubControlClient class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import threading
import atexit
import timeit
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
		self._discovery_completed = False;
		if self._context is None:
			self._context = zmq.Context.instance()
		self.closed = False
		self._require_subscribers = require_subscribers
		self._disable_pub = disable_pub
		self._sub_callback = sub_callback
		self._lock = threading.Lock()
		self._discover_lock = threading.Lock()
		self._push_sock = None
		self._pub_controller = None
		try:
			self._discover_uris()
		except ValueError:
			pass
		if self.push_uri or self.pub_uri:
			self.connect_zmq()
		_lock.acquire()
		_zmqpubcontrolclients.append(self)
		_lock.release()

	# The publish method for publishing the specified item to the specified
	# channel on the configured ZMQ endpoint. A non-blocking publish is
	# executed on a separate thread. Note that ZMQ publishes themselves are
	# non-blocking and will always result in a successful result being
	# sent to the callback (if a callback is specified). A failed publish
	# can only result from a failure to discover the PUSH / PUB URIs from
	# the command URI (if discovery occurs).
	def publish(self, channel, item, blocking=False, callback=None):
		self._verify_not_closed()
		if blocking:
			self._publish(channel, item, blocking, callback)
		else:
			thread = threading.Thread(target=self._publish,
				args=(channel, item, blocking, callback))
			thread.daemon = True
			thread.start()

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

	# An internal method for publishing the specified item to the specified
	# channel on the configured ZMQ endpoint. This method is meant to run
	# either synchronously or asynchronously depending on whether the blocking
	# parameter is set to true or false.
	def _publish(self, channel, item, blocking=False, callback=None):
		try:
			self._discover_uris()
			self.connect_zmq()
			if self._push_sock is None and self._pub_controller is None:
				if callback:
					callback(True, '')
				return
			i = item.export(True, True)
			channel = _ensure_utf8(channel)
			self._send_to_zmq(i, channel)
			if not blocking and callback:
				callback(True, '')
		except Exception as e:
			if not blocking and callback:
				callback(False, 'failed to publish: ' + str(e))
			elif blocking:
				raise ValueError('failed to publish: ' + str(e))

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
			content['channel'.encode('utf-8')] = channel
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
		self._discover_lock.acquire()
		if (self.uri is None or self._discovery_completed or
				(self.pub_uri and self.push_uri)):
			self._discover_lock.release()
			return
		sock = self._context.socket(zmq.REQ)
		sock.connect(self.uri)
		start = int(timeit.default_timer() * 1000)
		if not sock.poll(3000, zmq.POLLOUT):
			sock.close()
			self._discover_lock.release()
			raise ValueError('uri discovery request failed: pollout timeout')
		req = {'method'.encode('utf-8'): 'get-zmq-uris'.encode('utf-8')}
		sock.send(tnetstring.dumps(req))
		elapsed = max(int(timeit.default_timer() * 1000) - start, 0)
		if not sock.poll(max(3000 - elapsed, 0), zmq.POLLIN):
			sock.close()
			self._discover_lock.release()
			raise ValueError('uri discovery request failed: pollin timeout')
		resp = tnetstring.loads(sock.recv())
		sock.close()
		if (not resp.get('success'.encode('utf-8')) or
				not resp.get('value'.encode('utf-8'))):
			self._discover_lock.release()
			raise ValueError('uri discovery request failed: %s' % resp)
		self._set_discovered_uris(resp['value'.encode('utf-8')])		
		self._discovery_completed = True
		self._discover_lock.release()

	# An internal method for setting the URIs discovered via the command URI.
	# If the push and pub URIs were not explicitly set and neither was
	# discovered then an error will be raised.
	def _set_discovered_uris(self, discovery_result):
		command_host = self._get_command_host(self.uri)
		if (self.push_uri is None and
				'publish-pull'.encode('utf-8') in discovery_result):
			self.push_uri = self._resolve_uri(
					discovery_result['publish-pull'.encode('utf-8')], command_host)
		if (self.pub_uri is None and
					'publish-sub'.encode('utf-8') in discovery_result):
			self.pub_uri = self._resolve_uri(
					discovery_result['publish-sub'.encode('utf-8')], command_host)
		if self.push_uri is None and self.pub_uri is None:
			raise ValueError('uri discovery request failed: no uris discovered')

	# An internal method for getting the host from the specified URI.
	def _get_command_host(self, command_uri):
		command_host = None
		if command_uri[:6] == 'tcp://':
			at = command_uri.find(':', 6)
			command_host = command_uri[6:at]
		return command_host

	# An internal method for resolving a ZMQ URI when the URI contains an
	# asterisk representing all network interfaces.
	def _resolve_uri(self, uri, command_host):
		if uri[:6] == 'tcp://':
			at = uri.find(':', 6)
			addr = uri[6:at]
			if addr == '*':
				if command_host:
					return uri[0:6] + command_host + uri[at:]
				else:
					return uri[0:6] + 'localhost' + uri[at:]
		return uri

	# An internal method for determining whether discovery is still required
	# for the PUB socket.
	def _discovery_required_for_pub(self):
		return (self.pub_uri is None and self._require_subscribers and
				not self._discovery_completed)
