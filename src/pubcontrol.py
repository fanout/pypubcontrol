#    pubcontrol.py
#    ~~~~~~~~~
#    This module implements the PubControl class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import threading
import atexit
from .pcccbhandler import PubControlClientCallbackHandler
from .pubcontrolclient import PubControlClient
from .zmqpubcontrolclient import ZmqPubControlClient
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

# The global list of PubControl instances used to ensure that each instance
# is properly closed on exit.
_pubcontrols = list()
_lock = threading.Lock()

# An internal method used for closing all existing PubControl instances.
def _close_pubcontrols():
	_lock.acquire()
	pubcontrols = list(_pubcontrols)
	for pubcontrol in pubcontrols:
		pubcontrol.close()
	_lock.release()

# Register the _close_pubcontrols method with atexit to ensure that it is
# called on exit.
atexit.register(_close_pubcontrols)

# The PubControl class allows a consumer to manage a set of publishing
# endpoints and to publish to all of those endpoints via a single publish
# or publish_async method call. A PubControl instance can be configured
# either using a dict or array of dicts containing configuration information
# or by manually adding either PubControlClient or ZmqPubControlClient
# instances. Note that a PubControl instance that has been closed via the
# 'close' method will raise an exception if it is used.
class PubControl(object):

	# Initialize with or without a configuration. A configuration can be applied
	# after initialization via the apply_config method. Optionally specify a
	# subscription callback method that will be executed whenever a channel is 
	# subscribed to or unsubscribed from. The callback accepts two parameters:
	# the first parameter a string containing 'sub' or 'unsub' and the second
	# parameter containing the channel name. Optionally specify a ZMQ context
	# to use otherwise the global ZMQ context will be used.
	def __init__(self, config=None, sub_callback=None,
			zmq_context=None):
		self._lock = threading.Lock()
		self._sub_callback = sub_callback
		self._zmq_pub_controller = None
		self.clients = list()
		self.closed = False
		if config:
			self.apply_config(config)
		self._zmq_ctx = zmq_context
		if zmq_context is None and zmq:
			self._zmq_ctx = zmq.Context.instance()
		_lock.acquire()
		_pubcontrols.append(self)
		_lock.release()

	# Remove all of the configured client instances and close all open ZMQ sockets.
	def remove_all_clients(self):
		self._verify_not_closed()
		for client in self.clients:
			client.close()
		self.clients = list()

	# Add the specified PubControlClient or ZmqPubControlClient instance to
	# the list of clients.
	def add_client(self, client):
		self._verify_not_closed()
		self.clients.append(client)

	# Apply the specified configuration to this PubControl instance. The
	# configuration object can either be a dict or an array of dicts where
	# each dict corresponds to a single PubControlClient or ZmqPubControlClient
	# instance. Each dict will be parsed and a client instance will be created.
	# Specify a 'uri' dict key along with optional JWT authentication 'iss' and
	# 'key' dict keys for a PubControlClient configuration. Specify a combination
	# of 'zmq_uri', 'zmq_pub_uri', or 'zmq_push_uri' dict keys and an optional
	# 'zmq_require_subscribers' dict key for a ZmqPubControlClient configuration.
	def apply_config(self, config):
		self._verify_not_closed()
		if not isinstance(config, list):
			config = [config]
		clients = list()
		try:
			for entry in config:
				client = None
				if 'uri' in entry:
					client = PubControlClient(entry['uri'])
					if 'iss' in entry:
						client.set_auth_jwt({'iss': entry['iss']}, entry['key'])
				if ('zmq_uri' in entry or 'zmq_push_uri' in entry or
						'zmq_pub_uri' in entry):
					_verify_zmq()
					require_subscribers = entry.get('require_subscribers', False)
					client = ZmqPubControlClient(entry.get('zmq_uri'),
							entry.get('zmq_push_uri'), entry.get('zmq_pub_uri'),
							require_subscribers, True, None, self._zmq_ctx,
							self._discovery_callback)
				if client:
					clients.append(client)
		except:
			for client in clients:
				client.close()
			raise
		self.clients.extend(clients)

	# The publish method for publishing the specified item to the specified
	# channel on the configured endpoint. The blocking parameter indicates
	# whether the call should be blocking or non-blocking. The callback method
	# is optional and will be passed the publishing results after publishing is
	# complete. Note that a failure to publish in any of the configured
	# client instances will result in a failure result being passed to the
	# callback method along with the first encountered error message.
	def publish(self, channel, item, blocking=False, callback=None):
		self._verify_not_closed()
		cb = callback
		if not blocking:
			if callback:
				cb = PubControlClientCallbackHandler(len(self.clients),
						callback).handler
		for client in self.clients:
			client.publish(channel, item, blocking=blocking, callback=cb)
		self._send_to_zmq(channel, item)

	# The close method is a blocking call that closes all ZMQ sockets and
	# ensures that all PubControlClient async publishing is completed prior
	# to returning and allowing the consumer to proceed. Note that the
	# PubControl instance cannot be used after calling this method.
	def close(self):
		self._lock.acquire()
		self._verify_not_closed()
		for client in self.clients:
			client.close()
		if self._zmq_pub_controller:
			self._zmq_pub_controller.stop()
			self._zmq_pub_controller._thread.join()
			self._zmq_pub_controller = None
		_pubcontrols.remove(self)
		self.closed = True
		self._lock.release()

	# This method is a blocking method that ensures that all asynchronous
	# publishing is complete for all of the configured client instances prior
	# to returning and allowing the consumer to proceed.
	# NOTE: This only applies to PubControlClient and not ZmqPubControlClient
	# since all ZMQ socket operations are non-blocking.
	def wait_all_sent(self):
		self._verify_not_closed()
		for client in self.clients:
			client.wait_all_sent()

	# DEPRECATED: The finish method is now deprecated in favor of the more
	# descriptive wait_all_sent() method.
	def finish(self):
		self._verify_not_closed()
		self.wait_all_sent()

	# An internal method used as a callback for discovery within the ZMQ clients.
	# If a PUB URI was discovered then it is connected to via the ZmqPubController.
	def _discovery_callback(self, push_uri, pub_uri, require_subscribers):
		if pub_uri and require_subscribers:
			self._connect_zmq_pub_uri(pub_uri)

	# An internal method for connecting to a ZMQ PUB URI. If necessary a
	# ZmqPubController instance will be created. The ZmqPubController is
	# responsible for maintaining and publishing to the PUB socket.
	def _connect_zmq_pub_uri(self, uri):
		self._lock.acquire()
		if self._zmq_pub_controller is None:
			self._zmq_pub_controller = ZmqPubController(self._sub_callback,
					self._zmq_ctx)
		self._zmq_pub_controller.connect(_ensure_utf8(uri))
		self._lock.release()

	# An internal method for disconnecting from a ZMQ PUB URI.
	def _disconnect_zmq_pub_uri(self, uri):
		if self._zmq_pub_controller:
			self._lock.acquire()
			self._zmq_pub_controller.disconnect(_ensure_utf8(uri))
			self._lock.release()

	# An internal method for sending a ZMQ message for publishing to the
	# ZmqPubController.
	def _send_to_zmq(self, channel, item):
		self._lock.acquire()
		if self._zmq_pub_controller:
			channel = _ensure_utf8(channel)
			content = item.export(True, True)
			self._zmq_pub_controller.publish(channel,
					tnetstring.dumps(content))
		self._lock.release()

	# An internal method used as a callback for the ZmqPubController
	# instance. The purpose of this callback is to aggregate sub and unsub
	# events coming from the monitor and any clients that have their own
	# monitor. The consumer's sub_callback is executed when a channel is
	# first subscribed to for the first time across any clients or when a
	# channel is unsubscribed to across all clients.
	# NOTE: This method assumes that the ZmqPubController instance will
	# execute the callback: 1) before adding a subscription to its list
	# upon a 'sub' event, and 2) after removing a subscription from its
	# list upon an 'unsub' event.
	def _pub_controller_callback(self, eventType, chan):
		executeCallback = True
		for client in self.clients:
			if client._zmq_pub_controller:
				if chan in client._zmq_pub_controller.subscriptions:
					executeCallback = False
					break
		if (executeCallback and
				chan not in self._zmq_pub_controller.subscriptions):
			self._sub_callback(eventType, chan)

	# An internal method for verifying that the PubControl instance has
	# not been closed via the close() method. If it has then an error
	# is raised.
	def _verify_not_closed(self):
		if self.closed:
			raise ValueError('pubcontrol instance is closed')
