#    zmqpubcontroller.py
#    ~~~~~~~~~
#    This module implements the ZmqPubController class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import threading
from .utilities import _verify_zmq, _ensure_utf8

try:
	import zmq
except ImportError:
	zmq = None

# The ZmqPubController class facilitates the publishing of messages and the
# monitoring of subscriptions via ZMQ PUB sockets. It utilizes control and
# PUB ZMQ sockets where the control sockets provide a command interface while
# the PUB socket is used for message publishing and monitoring subscription
# information.
class ZmqPubController(object):

	# Initialize with a ZMQ context to use and callback where the callback
	# accepts two parameters: the first parameter a string containing 'sub'
	# or 'unsub' and the second parameter containing the subscription name.
	def __init__(self, callback, zmq_context=None):
		_verify_zmq()
		self.subscriptions = set()
		self._callback = callback
		self._context = zmq_context
		if self._context is None:
			self._context = zmq.Context.instance()
		self._stop_monitoring = False
		self._pub_sock = None
		self._control_uri = 'inproc://zmqpubcontroller-xpub-' + str(id(self))
		self._monitor_control_sock = None
		self._command_control_sock = self._context.socket(zmq.PAIR)
		self._command_control_sock.linger = 0
		self._command_control_sock.bind(self._control_uri)
		self._thread = threading.Thread(target=self._monitor)
		self._thread.daemon = True
		self._thread.start()

	# A method for connecting to the specified PUB URI by sending the 'connect'
	# message via the command control socket.
	def connect(self, uri):
		self._command_control_sock.send(_ensure_utf8('\x00') + uri)

	# A method for disconnecting from the specified PUB URI by sending the
	# 'disconnect' message via the command control socket.
	def disconnect(self, uri):
		self._command_control_sock.send(_ensure_utf8('\x01') + uri)

	# A method for sending the specified data to the PUB socket by sending the
	# 'publish' message via the command control socket.
	def publish(self, channel, content):
		self._command_control_sock.send(_ensure_utf8('\x02') + channel +
				_ensure_utf8('\x00') + content)

	# A method for stopping the monitoring done by this instance and closing
	# all sockets by sending the 'stop' message via the command control socket.
	def stop(self):
		self._command_control_sock.send(_ensure_utf8('\x03'))

	# This method is meant to run a separate thread and poll the ZMQ control
	# socket for control messages and the pub socket for subscribe and
	# unsubscribe events.
	def _monitor(self):
		self._poller = zmq.Poller()
		self._setup_monitor_control_socket()
		self._setup_pub_socket()
		while True:			
			if self._stop_monitoring:
				self._pub_sock.close()
				self._monitor_control_sock.close()
				self._command_control_sock.close()
				return
			socks = self._poller.poll()
			self._process_pub_sock_messages(socks)
			self._process_control_sock_messages(socks)

	# An internal method for setting up the control socket and connecting
	# it to the control socket URI.
	def _setup_monitor_control_socket(self):
		self._monitor_control_sock = self._context.socket(zmq.PAIR)
		self._monitor_control_sock.linger = 0
		self._monitor_control_sock.connect(self._control_uri)
		self._poller.register(self._monitor_control_sock, zmq.POLLIN)

	# An internal method for setting up the pub socket. This method does
	# not connect the socket to any endpoints. A 'connect' control message
	# received by the control socket triggers a connection.
	def _setup_pub_socket(self):
		self._pub_sock = self._context.socket(zmq.XPUB)
		self._pub_sock.linger = 0
		self._poller.register(self._pub_sock, zmq.POLLIN)

	# An internal method for processing the control socket messages. The
	# types of messages that can be processed are: 'connect', 'disconnect',
	# 'publish', and 'stop'.
	def _process_control_sock_messages(self, socks):
		if dict(socks).get(self._monitor_control_sock) == zmq.POLLIN:
			m = self._monitor_control_sock.recv()
			mtype = m[0]
			if mtype == '\x00':
				self._pub_sock.connect(m[1:])
			elif mtype == '\x01':
				self._pub_sock.disconnect(m[1:])
			elif mtype == '\x02':
				channel, content = m[1:].split('\x00', 1)
				self._pub_sock.send_multipart([channel, content])
			elif mtype == '\x03':
				self._stop_monitoring = True

	# An internal method for processing the pub socket messages. A subscribe
	# message appends the subscription to the subscriptions list and
	# executes the callback with a 'sub' event type, while an unsubscribe
	# message removes the subscription from the subscriptions list and executes
	# the callback with an 'unsub' event type.
	def _process_pub_sock_messages(self, socks):
		if dict(socks).get(self._pub_sock) == zmq.POLLIN:
			m = self._pub_sock.recv()
			mtype = m[0]
			item = m[1:]
			if mtype == '\x01':
				if item not in self.subscriptions:
					if self._callback:
						self._callback('sub', item)
					self.subscriptions.add(item)
			elif mtype == '\x00':
				if item in self.subscriptions:
					self.subscriptions.remove(item)
				if self._callback:
					self._callback('unsub', item)
