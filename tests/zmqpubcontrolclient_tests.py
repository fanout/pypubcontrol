import sys
import zmq
import tnetstring
import unittest
sys.path.append('../')
from src.item import Item
from src.format import Format
import src.zmqpubcontrolclient as zmqpcc

class TestFormatSubClass(Format):
	def name(self):
		return 'name'

	def export(self, tnetstring=False):
		self.tnetstring = True
		return {'body': 'bodyvalue'}

class ZmqSocketTestClass():
	def __init__(self):
		self.closed = True

	def close(self):
		self.closed = True

	def send(self, data):
		self.send_data = data

	def send_multipart(self, data):
		self.multipart_data = data

	def connect(self, uri):
		self.uri = uri

class ZmqPubControlClientTestClass(zmqpcc.ZmqPubControlClient):
	def connect_zmq(self):
		self.connect_zmq_called = True

	def _send_to_zmq(self, item, channel):
		self.send_item = item
		self.send_channel = channel

class ZmqPubControlClientTestClass2(zmqpcc.ZmqPubControlClient):
	def _verify_uri_config(self):
		self.verify_uri_config_called = True

class ZmqContextTestClass():
	def socket(self, socket_type):
		self.socket_type = socket_type
		return ZmqSocketTestClass()

class TestZmqPubControlClient(unittest.TestCase):
	def setUp(self):
		self.callback_result = None
		self.callback_message = None

	def test_initialize(self):
		client = zmqpcc.ZmqPubControlClient('uri')
		self.assertEquals(client.zmq_pub_uri, None)
		self.assertEquals(client.zmq_push_uri, None)
		self.assertEquals(client._require_subscribers, False)
		self.assertEquals(client._disable_pub, False)
		self.assertEquals(client._sub_callback, None)
		self.assertEquals(client._zmq_ctx, zmq.Context.instance())
		self.assertEquals(client._zmq_sock, None)
		self.assertEquals(client._sub_monitor, None)
		client = ZmqPubControlClientTestClass('uri', 'zmq_push_uri',
				'zmq_pub_uri', True, True, 'callback', 'context')
		self.assertEquals(client.zmq_pub_uri, 'zmq_pub_uri')
		self.assertEquals(client.zmq_push_uri, 'zmq_push_uri')
		self.assertEquals(client._require_subscribers, True)
		self.assertEquals(client._disable_pub, True)
		self.assertEquals(client._sub_callback, 'callback')
		self.assertEquals(client._zmq_ctx, 'context')
		self.assertEquals(client._zmq_sock, None)
		self.assertEquals(client._sub_monitor, None)
		self.assertTrue(client.connect_zmq_called)

	def callback(self, result, message):
		self.callback_result = result
		self.callback_message = message

	def test_publish(self):
		client = ZmqPubControlClientTestClass('uri', 'zmq_push_uri',
				'zmq_pub_uri', True, True, 'callback', 'context')
		client.publish('channel', Item(TestFormatSubClass()), True)
		self.assertTrue(client.connect_zmq_called)
		client = ZmqPubControlClientTestClass('uri', 'zmq_push_uri',
				'zmq_pub_uri', True, True, 'callback', 'context')
		client.publish('channel', Item(TestFormatSubClass()), False,
				self.callback)
		self.assertTrue(client.connect_zmq_called)
		self.assertEquals(self.callback_result, True)
		self.assertEquals(self.callback_message, '')
		client = ZmqPubControlClientTestClass('uri', 'zmq_push_uri',
				'zmq_pub_uri', True, True, 'callback', 'context')
		client._zmq_sock = 'sock'
		client.publish('channel', Item(TestFormatSubClass()), False,
				self.callback)
		self.assertTrue(client.connect_zmq_called)
		is_encoded = False
		try:
			if not isinstance(client.send_channel, unicode):
				is_encoded = True
		except NameError:
			if not isinstance(client.send_channel, str):
				is_encoded = True
		self.assertEqual(client.send_channel, 'channel'.encode('utf-8'))
		self.assertTrue(is_encoded)
		self.assertEquals(client.send_item,
				Item(TestFormatSubClass()).export(True, True))
		self.assertEquals(self.callback_result, True)
		self.assertEquals(self.callback_message, '')

	def test_close(self):
		client = zmqpcc.ZmqPubControlClient('uri')
		client.close()
		sock = ZmqSocketTestClass()
		client._zmq_sock = sock
		client.close()
		self.assertEquals(client._zmq_sock, None)
		self.assertTrue(sock.closed)

	def test_connect_zmq(self):
		client = ZmqPubControlClientTestClass2('uri')
		client.connect_zmq()
		self.assertTrue(client.verify_uri_config_called)
		client = ZmqPubControlClientTestClass2('uri')
		client.zmq_pub_uri = 'zmq_pub_uri'
		client._disable_pub = True
		client.connect_zmq()
		self.assertTrue(client.verify_uri_config_called)
		client = ZmqPubControlClientTestClass2('uri')
		client._zmq_ctx = ZmqContextTestClass()
		client.zmq_push_uri = 'zmq_push_uri'
		client.connect_zmq()
		self.assertTrue(client.verify_uri_config_called)
		self.assertEquals(client._zmq_ctx.socket_type, zmq.PUSH)
		self.assertEquals(client._zmq_sock.linger, 0)
		self.assertEquals(client._zmq_sock.uri, 'zmq_push_uri')
		client = ZmqPubControlClientTestClass2('uri')
		client._zmq_ctx = ZmqContextTestClass()
		client.zmq_pub_uri = 'zmq_pub_uri'
		client.connect_zmq()
		self.assertTrue(client.verify_uri_config_called)
		self.assertEquals(client._zmq_ctx.socket_type, zmq.XPUB)
		self.assertEquals(client._zmq_sock.linger, 0)
		self.assertEquals(client._zmq_sock.uri, 'zmq_pub_uri')
		self.assertEquals(client._sub_monitor, None)
		client = ZmqPubControlClientTestClass2('uri')
		client._zmq_ctx = ZmqContextTestClass()
		client._sub_callback = 'callback'
		client.zmq_pub_uri = 'zmq_pub_uri'
		client.connect_zmq()
		self.assertTrue(client.verify_uri_config_called)
		self.assertEquals(client._zmq_ctx.socket_type, zmq.XPUB)
		self.assertEquals(client._zmq_sock.linger, 0)
		self.assertEquals(client._zmq_sock.uri, 'zmq_pub_uri')
		self.assertNotEqual(client._sub_monitor, None)
		self.assertEquals(client._sub_monitor._lock, client._lock)
		self.assertEquals(client._sub_monitor._socket, client._zmq_sock)
		self.assertEquals(client._sub_monitor._callback, client._sub_callback)

	def test_verify_uri_config(self):
		client = zmqpcc.ZmqPubControlClient('uri')
		with self.assertRaises(ValueError):
			client._verify_uri_config()
		client = zmqpcc.ZmqPubControlClient('uri')
		client.zmq_push_uri = 'push_uri'
		client._verify_uri_config()
		client._require_subscribers = True
		with self.assertRaises(ValueError):
			client._verify_uri_config()

	def test_send_to_zmq(self):
		client = zmqpcc.ZmqPubControlClient('uri')
		client._zmq_sock = ZmqSocketTestClass()
		client._zmq_sock.socket_type = zmq.PUSH
		client._send_to_zmq({'content': 'content'}, 'chan')
		self.assertEquals(client._zmq_sock.send_data, tnetstring.dumps(
				{'content': 'content', 'channel': 'chan'}))
		client = zmqpcc.ZmqPubControlClient('uri')
		client._zmq_sock = ZmqSocketTestClass()
		client._zmq_sock.socket_type = zmq.XPUB
		client._send_to_zmq({'content': 'content'}, 'chan')
		self.assertEquals(client._zmq_sock.multipart_data, ['chan',
				tnetstring.dumps({'content': 'content'})])

if __name__ == '__main__':
		unittest.main()
