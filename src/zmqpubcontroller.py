#    zmqpubcontroller.py
#    ~~~~~~~~~
#    This module implements the ZmqPubController class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import zmq
import time
import threading

# The ZmqPubController class facilitates the monitoring of subscriptions via
# ZMQ PUB sockets.
class ZmqPubController(object):

	# Initialize with a control socket URI, ZMQ context to use, and callback
	# where the callback accepts two parameters: the first parameter a string
	# containing 'sub' or 'unsub' and the second parameter containing the
	# subscription name. The threading lock will be used relative to the
	# ZMQ socket operations.
	def __init__(self, control_sock_uri, callback,
			zmq_context=zmq.Context.instance()):
		self.subscriptions = list()
		self._control_sock_uri = control_sock_uri
		self._callback = callback
		self._context = zmq_context
		self._stop_monitoring = False
		self._pub_sock = None
		self._control_sock = None
		self._thread = threading.Thread(target=self._monitor)
		self._thread.daemon = True
		self._thread.start()

	# This method is meant to run a separate thread and poll the ZMQ control
	# socket for control messages and the pub socket for subscribe and
	# unsubscribe events. When an event is encountered then the callback is
	# executed with the event information.
	def _monitor(self):
		self._poller = zmq.Poller()
		self._setup_control_socket()
		self._setup_pub_socket()
		while True:			
			if self._stop_monitoring:
				self._pub_sock.close()
				self._control_sock.close()
				return
			print 'about to poll'
			socks = self._poller.poll()
			print 'returned from poll'
			self.process_pub_sock_messages(socks)
			self.process_control_sock_messages(socks)

	# An internal method for setting up the control socket and connecting
	# it to the control socket URI.
	def _setup_control_socket(self):
		self._control_sock = self._context.socket(zmq.PAIR)
		self._control_sock.linger = 0
		print self._control_sock_uri
		self._control_sock.connect(self._control_sock_uri)
		self._poller.register(self._control_sock, zmq.POLLIN)

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
	def _process_control_sock_messages(socks):
		if dict(socks).get(self._control_sock) == zmq.POLLIN:
			m = self._pub_sock.recv()
			mtype = m[0]
			if mtype == '\x00':
				self._zmq_sock.connect(m[1:])
			elif mtype == '\x01':
				self._zmq_sock.disconnect(m[1:])
			elif mtype == '\x02':
				data = m[1:].split('\x00')
				if len(data) == 2:				
					self._zmq_sock.send_multipart([data[0], data[1]])
			elif mtype == '\x03':
				self._stop_monitoring = True

	# An internal method for processing the pub socket messages. A subscribe
	# message appends the subscription to the subscriptions list and
	# executes the callback with a 'sub' event type, while an unsubscribe
	# message removes the subscription from the subscriptions list and executes
	# the callback with an 'unsub' event type.
	def _process_pub_sock_messages(socks):
		if dict(socks).get(self._pub_sock) == zmq.POLLIN:
			m = self._pub_sock.recv()
			mtype = m[0]
			item = m[1:]
			if mtype == '\x01':
				if item not in self.subscriptions:
					if self._callback:
						self._callback('sub', item)
					self.subscriptions.append(item)
			elif mtype == '\x00':
				if item in self.subscriptions:
					self.subscriptions.remove(item)
				if self._callback:
					self._callback('unsub', item)
