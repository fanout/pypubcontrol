import sys
import zmq
import tnetstring
import unittest
import time
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

pollin_response = 1
pollout_response = 1

class ZmqSocketTestClass():
	def __init__(self, ):
		self.closed = True
		self.poll_params = list()

	def close(self):
		self.closed = True

	def send(self, data):
		self.send_data = data

	def send_multipart(self, data):
		self.multipart_data = data

	def connect(self, uri):
		self.connect_uri = uri

	def bind(self, uri):
		self.bind_uri = uri

	def poll(self, poll_timeout, poll_type):
		self.poll_params.append((poll_timeout, poll_type))
		if poll_type == zmq.POLLIN:
			return pollin_response
		else:
			time.sleep(1)
			return pollout_response

	def recv(self):
		return tnetstring.dumps({'success': True,
				'value': {'publish-pull': 'publish-pull',
				'publish-sub': 'publish-sub'}})

class ZmqSocketTestClass2():
	def close(self):
		self.closed = True

	def send(self, data):
		self.send_data = data

	def connect(self, uri):
		self.connect_uri = uri

	def poll(self, poll_timeout, poll_type):
		return 1

	def recv(self):
		return tnetstring.dumps({'success': False})

class ZmqPubControlClientTestClass(zmqpcc.ZmqPubControlClient):
	def _verify_not_closed(self):
		self.verify_not_closed_called = True

	def _discover_uris(self):
		self.discover_called = True

	def connect_zmq(self):
		self.connect_called = True

	def _send_to_zmq(self, item, channel):
		self.send_item = item
		self.send_channel = channel

class ZmqPubControlClientTestClass2(zmqpcc.ZmqPubControlClient):
	def _verify_uri_config(self):
		self.verify_uri_config_called = True

	def _discover_uris(self):
		self.discover_called = True
		raise ValueError()

	def close(self):
		self.closed = True

class ZmqPubControlClientTestClass3(zmqpcc.ZmqPubControlClient):
	def _discover_uris(self):
		self.discover_called = True

	def close(self):
		self.closed = True

class ZmqPubControlClientTestClass4(zmqpcc.ZmqPubControlClient):
	def connect_zmq(self):
		self.connect_called = True

	def _resolve_uri(self, uri, host):
		try:
			self.resolve_uris.append(uri)
			self.resolve_hosts.append(host)
		except:
			self.resolve_uris = list()
			self.resolve_hosts = list()
			self.resolve_uris.append(uri)
			self.resolve_hosts.append(host)
		return uri

	def close(self):
		self.closed = True

class ZmqPubControlClientTestClass5(zmqpcc.ZmqPubControlClient):
	def connect_zmq(self):
		self.connect_called = True

	def _discover_uris(self):
		self.discover_called = True

	def close(self):
		self.closed = True

	def _get_command_host(self, uri):
		self.command_uri = uri

class ZmqContextTestClass():
	def socket(self, socket_type):
		self.socket_type = socket_type
		self.last_socket_created = ZmqSocketTestClass()
		return self.last_socket_created

class ZmqContextTestClass2():
	def socket(self, socket_type):
		self.socket_type = socket_type
		self.last_socket_created = ZmqSocketTestClass2()
		return self.last_socket_created

class ThreadTestClass():
	def join(self):
		self.join_called = True

class ZmqPubControllerTestClass():
	def __init__(self, callback=None, context=None):
		self.callback = callback
		self.context = context

	def publish(self, channel, content):
		self.publish_channel = channel
		self.publish_content = content

	def connect(self, uri):
		self.connect_uri = uri

	def stop(self):
		self.stop_called = True

class TestZmqPubControlClient(unittest.TestCase):
	def setUp(self):
		self.callback_result = None
		self.callback_message = None
		self.verify_zmq_called = False

	def verify_zmq(self):
		self.verify_zmq_called = True

	def test_close_zmqpubcontrolclients(self):
		zmqpcc._zmqpubcontrolclients = list()
		client1 = ZmqPubControlClientTestClass2('uri')
		client2 = ZmqPubControlClientTestClass2('uri')
		zmqpcc._zmqpubcontrolclients.append(client1)
		zmqpcc._zmqpubcontrolclients.append(client2)
		zmqpcc._close_zmqpubcontrolclients()
		self.assertTrue(client1.closed)
		self.assertTrue(client2.closed)

	def test_initialize(self):
		zmqpcc._verify_zmq = self.verify_zmq
		client = ZmqPubControlClientTestClass('uri')
		self.assertEquals(client.uri, 'uri')
		self.assertEquals(client.pub_uri, None)
		self.assertEquals(client.push_uri, None)
		self.assertEquals(client._push_sock, None)
		self.assertEquals(client._require_subscribers, False)
		self.assertEquals(client._disable_pub, False)
		self.assertEquals(client._sub_callback, None)
		self.assertEquals(client._context, zmq.Context.instance())
		self.assertEquals(client._pub_controller, None)
		self.assertFalse(client._discovery_completed)
		self.assertFalse(client.closed)
		self.assertTrue(client.discover_called)
		self.assertFalse(hasattr(client, 'connect_called'))
		self.assertTrue(client in zmqpcc._zmqpubcontrolclients)
		self.assertTrue(self.verify_zmq_called)
		client = ZmqPubControlClientTestClass(None, 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		self.assertEquals(client.uri, None)
		self.assertEquals(client.pub_uri, 'pub_uri')
		self.assertEquals(client.push_uri, 'push_uri')
		self.assertEquals(client._push_sock, None)
		self.assertEquals(client._require_subscribers, True)
		self.assertEquals(client._disable_pub, True)
		self.assertEquals(client._sub_callback, 'callback')
		self.assertEquals(client._context, 'context')
		self.assertEquals(client._pub_controller, None)
		self.assertFalse(client._discovery_completed)
		self.assertTrue(client in zmqpcc._zmqpubcontrolclients)
		self.assertFalse(client.closed)
		self.assertTrue(client.discover_called)
		self.assertTrue(client.connect_called)
		self.assertTrue(self.verify_zmq_called)

	def callback(self, result, message):
		self.callback_result = result
		self.callback_message = message

	def test_publish(self):
		client = ZmqPubControlClientTestClass('uri', 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		client.connect_called = False
		client.publish('channel', Item(TestFormatSubClass()), True)
		self.assertTrue(client.connect_called)
		self.assertTrue(client.verify_not_closed_called)
		self.assertFalse(hasattr(client, 'send_item'))
		client = ZmqPubControlClientTestClass('uri', 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		client.connect_called = False
		client.publish('channel', Item(TestFormatSubClass()), False,
				self.callback)
		self.assertFalse(hasattr(client, 'send_item'))
		self.assertTrue(client.connect_called)
		self.assertTrue(client.verify_not_closed_called)
		self.assertEquals(self.callback_result, True)
		self.assertEquals(self.callback_message, '')
		client = ZmqPubControlClientTestClass('uri', 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		client._push_sock = ZmqSocketTestClass()
		client.publish('channel', Item(TestFormatSubClass()), False,
				self.callback)
		self.assertTrue(client.connect_called)
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
		client = ZmqPubControlClientTestClass('uri')
		self.assertTrue(client in zmqpcc._zmqpubcontrolclients)
		client.close()
		self.assertFalse(client in zmqpcc._zmqpubcontrolclients)
		self.assertTrue(client.verify_not_closed_called)
		client = ZmqPubControlClientTestClass('uri')
		sock = ZmqSocketTestClass()
		client._push_sock = sock
		pubcontroller = ZmqPubControllerTestClass()
		pubcontroller._thread = ThreadTestClass()
		client._pub_controller = pubcontroller		
		client.close()
		self.assertEquals(client._push_sock, None)
		self.assertTrue(sock.closed)
		self.assertTrue(pubcontroller.stop_called)
		self.assertTrue(pubcontroller._thread.join_called)

	def test_connect_zmq(self):
		client = ZmqPubControlClientTestClass2('uri')
		client._require_subscribers = True
		client._disable_pub = True
		client.connect_zmq()
		self.assertEquals(client._push_sock, None)
		self.assertEquals(client._pub_controller, None)
		client = ZmqPubControlClientTestClass2('uri')
		client._context = ZmqContextTestClass()
		client._require_subscribers = False
		client.push_uri = 'push_uri'
		client.connect_zmq()
		self.assertEquals(client._context.socket_type, zmq.PUSH)
		self.assertEquals(client._push_sock.linger, 0)
		self.assertEquals(client._push_sock.connect_uri, 'push_uri')
		client = ZmqPubControlClientTestClass2('uri')
		client._context = ZmqContextTestClass()
		client._require_subscribers = True
		client._sub_callback = 'callback'
		client.pub_uri = 'pub_uri'
		zmqpcc.ZmqPubController = ZmqPubControllerTestClass
		client.connect_zmq()
		self.assertEquals(client._pub_controller.callback, 'callback')
		self.assertEquals(client._pub_controller.context, client._context)
		self.assertEquals(client._pub_controller.connect_uri, 'pub_uri')

	def test_verify_uri_config(self):
		client = ZmqPubControlClientTestClass3('uri')
		with self.assertRaises(ValueError):
			client._verify_uri_config()
		client = ZmqPubControlClientTestClass3('uri')
		client.push_uri = 'push_uri'
		client._verify_uri_config()
		client._require_subscribers = True
		with self.assertRaises(ValueError):
			client._verify_uri_config()
		client = ZmqPubControlClientTestClass3('uri')
		client.pub_uri = 'pub_uri'
		with self.assertRaises(ValueError):
			client._verify_uri_config()
		client = ZmqPubControlClientTestClass3('uri')
		client.push_uri = 'push_uri'
		client._sub_callback = 'callback'
		with self.assertRaises(ValueError):
			client._verify_uri_config()
		client.pub_uri = 'pub_uri'
		client._require_subscribers = True
		client._verify_uri_config()

	def test_send_to_zmq(self):
		client = ZmqPubControlClientTestClass2('uri')
		client._push_sock = ZmqSocketTestClass()
		client._send_to_zmq({'content': 'content'}, 'chan')
		self.assertEquals(client._push_sock.send_data, tnetstring.dumps(
				{'content': 'content', 'channel': 'chan'}))
		client = ZmqPubControlClientTestClass2('uri')
		client._pub_controller = ZmqPubControllerTestClass()
		client._send_to_zmq({'content': 'content'}, 'chan')
		self.assertEquals(client._pub_controller.publish_channel, 'chan')
		self.assertEquals(client._pub_controller.publish_content,
				tnetstring.dumps({'content': 'content'}))

	def test_verify_not_closed(self):
		client = ZmqPubControlClientTestClass2('uri')
		client._verify_not_closed()
		client.closed = True
		with self.assertRaises(ValueError):
			client._verify_not_closed()

	def test_discover_uris(self):
		global pollin_response
		global pollout_response
		time.clock = time.time
		client = ZmqPubControlClientTestClass4('uri', 'push_uri', 'pub_uri')
		client._discover_uris()
		self.assertFalse(client._discovery_completed)
		client = ZmqPubControlClientTestClass4('uri', 'push_uri', 'pub_uri')
		client.uri = None
		client.pub_uri = None
		client.push_uri = None
		client._discover_uris()
		self.assertFalse(client._discovery_completed)
		client = ZmqPubControlClientTestClass4('tcp://localhost:5563',
				'push_uri', 'pub_uri')
		client.pub_uri = None
		client.push_uri = None
		client._discovery_completed = True
		client._discover_uris()
		self.assertTrue(client._discovery_completed)
		self.assertFalse(hasattr(client._context, '.last_socket_created'))
		client = ZmqPubControlClientTestClass4('tcp://localhost:5563',
				'push_uri', 'pub_uri')
		client.pub_uri = None
		client.push_uri = None
		pollin_response = 1
		pollout_response = 1
		client._context = ZmqContextTestClass()
		client._discover_uris()
		self.assertTrue(client._discovery_completed)
		self.assertEquals(client._context.socket_type, zmq.REQ)
		self.assertEquals(client._context.last_socket_created.
				connect_uri, 'tcp://localhost:5563')
		self.assertEquals(client._context.last_socket_created.
				poll_params[0][0], 3000)
		self.assertEquals(client._context.last_socket_created.
				poll_params[0][1], zmq.POLLOUT)
		self.assertTrue(client._context.last_socket_created.
				poll_params[1][0] < 2000 and
				client._context.last_socket_created.poll_params[1][0] > 1950)
		self.assertEquals(client._context.last_socket_created.
				poll_params[1][1], zmq.POLLIN)
		self.assertTrue(client._context.last_socket_created.closed)
		self.assertEquals(client._context.last_socket_created.send_data,
				tnetstring.dumps({'method': 'get-zmq-uris'}))
		self.assertEquals(client.resolve_uris[0], 'publish-pull')
		self.assertEquals(client.resolve_uris[1], 'publish-sub')
		self.assertEquals(client.resolve_hosts[0], 'localhost')
		self.assertEquals(client.resolve_hosts[1], 'localhost')
		self.assertEquals(client.push_uri, 'publish-pull')
		self.assertEquals(client.pub_uri, 'publish-sub')
		client = ZmqPubControlClientTestClass4('tcp://localhost:5563',
				'push_uri', 'pub_uri')
		client.pub_uri = None
		client.push_uri = None
		client._context = ZmqContextTestClass()
		pollin_response = 1
		pollout_response = 0
		with self.assertRaises(ValueError):
			client._discover_uris()
		self.assertFalse(client._discovery_completed)
		self.assertEquals(client._context.last_socket_created.
				poll_params[0][0], 3000)
		self.assertEquals(client._context.last_socket_created.
				poll_params[0][1], zmq.POLLOUT)
		self.assertTrue(client._context.last_socket_created.closed)
		client = ZmqPubControlClientTestClass4('tcp://localhost:5563',
				'push_uri', 'pub_uri')
		client.pub_uri = None
		client.push_uri = None
		client._context = ZmqContextTestClass()
		pollin_response = 0
		pollout_response = 1
		with self.assertRaises(ValueError):
			client._discover_uris()
		self.assertFalse(client._discovery_completed)
		self.assertEquals(client._context.last_socket_created.
				poll_params[0][0], 3000)
		self.assertEquals(client._context.last_socket_created.
				poll_params[0][1], zmq.POLLOUT)
		self.assertTrue(client._context.last_socket_created.closed)
		pollin_response = 1
		pollout_response = 1
		client = ZmqPubControlClientTestClass4('uri',
				'push_uri', 'pub_uri')
		client.pub_uri = None
		client.push_uri = None
		client._context = ZmqContextTestClass()
		client._discover_uris()
		self.assertEquals(client._context.socket_type, zmq.REQ)
		self.assertEquals(client._context.last_socket_created.
				connect_uri, 'uri')
		self.assertTrue(client._context.last_socket_created.closed)
		self.assertEquals(client.resolve_uris[0], 'publish-pull')
		self.assertEquals(client.resolve_uris[1], 'publish-sub')
		self.assertEquals(client.resolve_hosts[0], None)
		self.assertEquals(client.resolve_hosts[1], None)
		client = ZmqPubControlClientTestClass4('tcp://localhost:5563',
				'push_uri', 'pub_uri')
		client.pub_uri = None
		client.push_uri = None
		client._context = ZmqContextTestClass2()
		client.resp = False
		with self.assertRaises(ValueError):
			client._discover_uris()
		self.assertTrue(client._context.last_socket_created.closed)

	def test_set_discovered_uris(self):
		client = ZmqPubControlClientTestClass5('uri',
				'push_uri', 'pub_uri')
		result = {'publish-pull': 'push', 'publish-sub': 'pub'}
		client._set_discovered_uris(result)
		self.assertEqual(client.push_uri, 'push_uri')
		self.assertEqual(client.pub_uri, 'pub_uri')
		self.assertEquals(client.command_uri, 'uri')
		client.push_uri = None
		client.pub_uri = None
		client._set_discovered_uris(result)
		self.assertEqual(client.push_uri, 'push')
		self.assertEqual(client.pub_uri, 'pub')
		self.assertEquals(client.command_uri, 'uri')
		client.push_uri = None
		client.pub_uri = None
		result = {}
		with self.assertRaises(ValueError):
			client._set_discovered_uris(result)

	def test_get_command_host(self):
		client = ZmqPubControlClientTestClass4('uri',
				'push_uri', 'pub_uri')
		self.assertEquals(client._get_command_host('uri'), None)
		self.assertEquals(client._get_command_host('tcp://host:5000'), 'host')
		self.assertEquals(client._get_command_host('tcp://a:5000'), 'a')

	def test_resolve_uri(self):
		client = ZmqPubControlClientTestClass('uri')
		self.assertEquals(client._resolve_uri('uri', 'host'), 'uri')
		self.assertEquals(client._resolve_uri('tcp://localhost:5000',
			'host'), 'tcp://localhost:5000')
		self.assertEquals(client._resolve_uri('tcp://*:5000',
			'host'), 'tcp://host:5000')
		self.assertEquals(client._resolve_uri('tcp://*:5000',
			None), 'tcp://localhost:5000')

if __name__ == '__main__':
		unittest.main()
