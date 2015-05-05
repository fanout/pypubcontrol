import sys
import unittest
import time
import threading
import zmq
sys.path.append('../')
import src.zmqpubcontroller as zmqpubcontroller

class ZmqPubControllerTestClass(zmqpubcontroller.ZmqPubController):
	def _monitor(self):
		self.monitor_started = True

class ZmqPollerTestClass(object):
	def __init__(self):
		self.register_data = list()

	def register(self, socket, pollType):
		self.register_data.append((socket, pollType))

	def poll(self):
		return {control_socket: 'pollin', pub_socket: 'pollin'}

class ZmqTestClass(object):
	def __init__(self):
		self.PAIR = 1
		self.XPUB = 2
		self.POLLIN = 'pollin'

	def Poller(self):
		return ZmqPollerTestClass()

class PubSocketTestClass(object):
	def __init__(self):
		self.count = 0
		self.closed = False

	def disconnect(self, uri):
		self.disconnect_uri = uri

	def connect(self, uri):
		self.connect_uri = uri

	def send_multipart(self, data):
		self.pub_data = data

	def close(self):
		self.close_called = True

	def recv(self):
		self.count += 1
		if self.count == 1:
			return '\x01chan'
		if self.count == 2:
			return '\x01chan2'
		if self.count == 3:
			return '\x01chan'
		if self.count == 4:
			self.closed = True
			return '\x00chan'

class ControlSocketTestClass(object):
	def __init__(self):
		self.count = 0
		self.closed = False

	def connect(self, uri):
		self.connect_uri = uri

	def close(self):
		self.close_called = True

	def recv(self):
		self.count += 1
		if self.count == 1:
			return '\x00uri2'
		if self.count == 2:
			return '\x01uri3'
		if self.count == 3:
			return '\x02chan\x00pub'
		if self.count == 4:
			self.closed = True
			return '\x03'

pub_socket = PubSocketTestClass()
control_socket = ControlSocketTestClass()

class ZmqContextTestClass():
	def socket(self, socket_type):
		if socket_type == 1:
			return control_socket
		else:
			return pub_socket

class TestZmqPubController(unittest.TestCase):
	def setUp(self):
		self.eventCount = 0

	def test_initialize(self):
		mon = ZmqPubControllerTestClass('uri', 'callback', 'context')
		self.assertEqual(len(mon.subscriptions), 0)
		self.assertEqual(mon._control_sock_uri, 'uri')
		self.assertEqual(mon._callback, 'callback')
		self.assertEqual(mon._context, 'context')
		self.assertEqual(mon._stop_monitoring, False)
		self.assertEqual(mon._pub_sock, None)
		self.assertEqual(mon._control_sock, None)
		self.assertEqual(mon._thread.daemon, True)
		time.sleep(1)
		self.assertTrue(mon.monitor_started)

	def test_monitor(self):	
		zmqpubcontroller.zmq = ZmqTestClass()
		mon = zmqpubcontroller.ZmqPubController('uri',
				self.sub_callback, ZmqContextTestClass())
		self.assertTrue(mon._thread.daemon)
		time.sleep(2)
		self.assertEqual(mon._poller.register_data[0][0], mon._control_sock)
		self.assertEqual(mon._poller.register_data[0][1], 'pollin')
		self.assertEqual(mon._poller.register_data[1][0], mon._pub_sock)
		self.assertEqual(mon._poller.register_data[1][1], 'pollin')
		self.assertEqual(mon._control_sock.connect_uri, 'uri')
		self.assertEqual(mon._control_sock.linger, 0)
		self.assertEqual(mon._control_sock.close_called, True)
		self.assertEqual(mon._pub_sock.linger, 0)
		self.assertEqual(mon._pub_sock.connect_uri, 'uri2')
		self.assertEqual(mon._pub_sock.disconnect_uri, 'uri3')
		self.assertEqual(mon._pub_sock.pub_data, ['chan', 'pub'])
		self.assertEqual(mon._pub_sock.close_called, True)
		self.assertFalse(mon._thread.isAlive())
		self.assertEqual(len(mon.subscriptions), 1)
		self.assertEqual(mon.subscriptions[0], 'chan2')
		self.assertEqual(self.eventCount, 3)

	def sub_callback(self, eventType, item):
		self.eventCount += 1
		if self.eventCount == 1:
			self.assertEqual(eventType, 'sub')
			self.assertEqual(item, 'chan')
		if self.eventCount == 2:
			self.assertEqual(eventType, 'sub')
			self.assertEqual(item, 'chan2')
		if self.eventCount == 3:
			self.assertEqual(eventType, 'unsub')
			self.assertEqual(item, 'chan')

if __name__ == '__main__':
		unittest.main()
