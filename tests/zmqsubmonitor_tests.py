import sys
import unittest
import time
import threading
sys.path.append('../')
import src.zmqsubmonitor as zmqsubmonitor

class ZmqSubMonitorTestClass(zmqsubmonitor.ZmqSubMonitor):
	def _monitor(self):
		self.monitor_started = True

class ZmqPollerTestClass(object):
	def register(self, socket, pollType):
		pass

	def poll(self):
		return {socket: 'pollin'}

class ZmqTestClass(object):
	def __init__(self):
		self.POLLIN = 'pollin'

	def Poller(self):
		return ZmqPollerTestClass()

class ZmqSocketTestClass(object):
	def __init__(self):
		self.count = 0
		self.closed = False

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

socket = ZmqSocketTestClass()

class TestZmqSubMonitor(unittest.TestCase):
	def setUp(self):
		self.eventCount = 0

	def test_initialize(self):
		mon = ZmqSubMonitorTestClass('sock', 'lock', 'callback')
		self.assertEqual(len(mon.subscriptions), 0)
		self.assertEqual(mon._lock, 'lock')
		self.assertEqual(mon._socket, 'sock')
		self.assertEqual(mon._callback, 'callback')
		time.sleep(1)
		self.assertTrue(mon.monitor_started)

	def test_monitor(self):	
		zmqsubmonitor.zmq = ZmqTestClass()
		mon = zmqsubmonitor.ZmqSubMonitor(socket,
				threading.Lock(), self.sub_callback)
		time.sleep(2)
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
