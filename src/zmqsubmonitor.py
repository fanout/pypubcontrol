#    zmqsubmonitor.py
#    ~~~~~~~~~
#    This module implements the features that monitor channel
#    subscriptions from ZMQ PUB sockets.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import zmq
import time
import threading

# The ZmqSubMonitor class facilitates the monitoring of channel subscriptions
# via ZMQ PUB sockets. This class is used by the ZmqPubControlClient and
# PubControl classes and should not be used directly.
class ZmqSubMonitor(object):

	def __init__(self, socket):
		self.channels = set()
		self._socket = socket
		self._thread = threading.Thread(target=self._monitor)
		self._thread.daemon = True
		self._thread.start()
		
	def _monitor(self):
		poller = zmq.Poller()
		poller.register(self._socket, zmq.POLLIN)
		while True:
			if self._socket.closed:
				return
			# TODO: Do we need to try - except for socket closed errors?
			socks = dict(poller.poll())
			if socks.get(self._socket) == zmq.POLLIN:
				m = self._socket.recv()
				mtype = m[0]
				channel = m[1:]
				if mtype == '\x01':
					print 'subscribed to ' + channel
					self.channels.add(channel)
				elif mtype == '\x00':
					print 'unsubscribed from ' + channel
					self.channels.remove(channel)
