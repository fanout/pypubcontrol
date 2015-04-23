#    zmqpubcontrolclient.py
#    ~~~~~~~~~
#    This module implements the ZmqPubControlClient class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import zmq
import tnetstring
import threading

# The ZmqPubControlClient class allows consumers to publish either synchronously 
# or asynchronously to a ZMQ endpoint of their choice. The consumer wraps a Format
# class instance in an Item class instance and passes that to the publish
# methods. The async publish method has an optional callback parameter that
# is called after the publishing is complete to notify the consumer of the
# result.
class ZmqPubControlClient(object):

	# Initialize this class with a URL representing the REQ socket endpoint
	# and an optional PUB sock endpoint.
	def __init__(self, uri, zmq_pub_uri=None):
		self.uri = uri
		self.zmq_pub_uri = zmq_pub_uri
		self._lock = threading.Lock()
		self._zmq_ctx = None
		self._zmq_sock = None

	# The publish method for publishing the specified item to the specified
	# channel on the configured endpoint. The blocking parameter indicates
	# whether the the callback method should be blocking or non-blocking. The
	# callback parameter is optional and will be passed the publishing results
	# after publishing is complete. Note that the callback executes on a
	# separate thread.
	def publish(self, channel, item, blocking=False, callback=None):
		assert(self.zmq_pub_uri)
		self._connect_zmq(self.zmq_pub_uri)
		i = item.export(True, True)
		i['channel'] = channel
		content = dict()
		content['items'] = [i]
		content_raw = tnetstring.dumps(content)
		try:
			if isinstance(content_raw, unicode):
				content_raw = content_raw.encode('utf-8')
		except NameError:
			if isinstance(content_raw, str):
				content_raw = content_raw.encode('utf-8')
		self._send_to_zmq(content_raw)
		if callback:
			callback(True, '')

	# The finish method is a blocking method that ensures that all asynchronous
	# publishing is complete prior to returning and allowing the consumer to 
	# proceed.
	def finish(self):
		pass

	# An internal method for setting up and connecting to the ZMQ endpoint.
	def _connect_zmq(self, uri):		
		self._lock.acquire()
		if self._zmq_ctx is None and self._zmq_sock is None:
			self._zmq_ctx = zmq.Context()
			self._zmq_sock = self._zmq_ctx.socket(zmq.PUSH)
			self._zmq_sock.linger = 0
			self._zmq_sock.connect(uri)
		self._lock.release()
		
	# An internal method for publishing the ZMQ message to the specified URI
	# with the specified content.
	def _send_to_zmq(self, content):
		self._zmq_sock.send(content)
