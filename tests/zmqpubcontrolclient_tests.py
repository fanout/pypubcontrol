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

	def export(self):
		return {'body': 'bodyvalue'}

pollin_response = 1
pollout_response = 1
successful_discovery_result = {'success'.encode('utf-8'): True,
		'value'.encode('utf-8'): {'publish-pull'.encode('utf-8'):
		'publish-pull'.encode('utf-8'),
		'publish-sub'.encode('utf-8'): 'publish-sub'.encode('utf-8')}}
failed_discovery_result1 = {'success'.encode('utf-8'): False}
failed_discovery_result2 = {'success'.encode('utf-8'): True}
discovery_result = successful_discovery_result

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
		return tnetstring.dumps(discovery_result)

class ZmqPubControlClientTestClass(zmqpcc.ZmqPubControlClient):
	def _verify_not_closed(self):
		self.verify_not_closed_called = True

	def _discover_uris(self):
		self.discover_called = True

	def connect_zmq(self):
		self.connect_called = True

	def _verify_uri_config(self):
		self.verify_uri_config_called = True

	def _send_to_zmq(self, item, channel):
		self.send_item = item
		self.send_channel = channel
		if hasattr(self, 'raise_during_send') and self.raise_during_send:
			raise ValueError()		

class ZmqPubControlClientTestClass2(zmqpcc.ZmqPubControlClient):
	def _verify_not_closed(self):
		self.verify_not_closed_called = True

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

class ZmqPubControlClientTestClass6(zmqpcc.ZmqPubControlClient):
	def _verify_not_closed(self):
		self.verify_not_closed_called = True

	def _discover_uris(self):
		self.discover_called = True

	def connect_zmq(self):
		self.connect_called = True

	def _verify_uri_config(self):
		self.verify_uri_config_called = True

	def _send_to_zmq(self, item, channel):
		self.send_item = item
		self.send_channel = channel

	def _publish(self, channel, item, blocking, callback):
		self.publish_channel = channel
		self.publish_item = item
		self.publish_blocking = blocking
		self.publish_callback = callback

class ZmqContextTestClass():
	def socket(self, socket_type):
		self.socket_type = socket_type
		self.last_socket_created = ZmqSocketTestClass()
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
		self.assertNotEqual(client._lock, None)
		self.assertNotEqual(client._thread_cond, None)
		self.assertEquals(client._push_sock, None)
		self.assertEquals(client._require_subscribers, False)
		self.assertEquals(client._disable_pub, False)
		self.assertEquals(client._sub_callback, None)
		self.assertEquals(client._context, zmq.Context.instance())
		self.assertEquals(client._pub_controller, None)
		self.assertEquals(client._discovery_callback, None)
		self.assertFalse(client._discovery_completed)
		self.assertFalse(client.closed)
		self.assertFalse(hasattr(client, 'connect_called'))
		self.assertTrue(client in zmqpcc._zmqpubcontrolclients)
		self.assertTrue(self.verify_zmq_called)
		time.sleep(0.5)
		self.assertTrue(client.discover_called)
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
		self.assertTrue(client in zmqpcc._zmqpubcontrolclients)
		self.assertFalse(client.closed)
		self.assertTrue(client.connect_called)
		self.assertTrue(self.verify_zmq_called)
		self.assertTrue(client.verify_uri_config_called)
		time.sleep(0.5)
		self.assertTrue(client.discover_called)

	def callback(self, result, message):
		self.callback_result = result
		self.callback_message = message

	def test_publish(self):
		client = ZmqPubControlClientTestClass6('uri', 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		client.publish('chan', 'item', True, 'callback')
		self.assertEquals(client.publish_channel, 'chan')
		self.assertEquals(client.publish_item, 'item')
		self.assertEquals(client.publish_blocking, True)
		self.assertEquals(client.publish_callback, 'callback')
		client = ZmqPubControlClientTestClass6('uri', 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		client.publish('chan', 'item', False, 'callback')
		time.sleep(0.5)
		self.assertEquals(client.publish_channel, 'chan')
		self.assertEquals(client.publish_item, 'item')
		self.assertEquals(client.publish_blocking, False)
		self.assertEquals(client.publish_callback, 'callback')

	def test_publish_internal(self):
		client = ZmqPubControlClientTestClass('uri', 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		client.discover_called = False
		client._publish('channel', Item(TestFormatSubClass()), True, None)
		self.assertTrue(client.discover_called)		
		self.assertTrue(client.verify_uri_config_called)
		self.assertFalse(hasattr(client, 'send_item'))
		client = ZmqPubControlClientTestClass('uri', 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		client.discover_called = False
		client._publish('channel', Item(TestFormatSubClass()), False,
				self.callback)
		self.assertFalse(hasattr(client, 'send_item'))
		self.assertTrue(client.discover_called)
		self.assertTrue(client.verify_uri_config_called)
		self.assertEquals(self.callback_result, True)
		self.assertEquals(self.callback_message, '')
		self.callback_result = None
		self.callback_message = None
		client = ZmqPubControlClientTestClass('uri', 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		client.discover_called = False
		client._push_sock = ZmqSocketTestClass()
		client._publish('channel', Item(TestFormatSubClass()), False,
				self.callback)
		self.assertTrue(client.discover_called)
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
		self.callback_result = None
		self.callback_message = None
		client = ZmqPubControlClientTestClass('uri', 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		client.raise_during_send = True
		client.discover_called = False
		client._push_sock = ZmqSocketTestClass()
		client._publish('channel', Item(TestFormatSubClass()), False,
				self.callback)
		self.assertTrue(client.discover_called)
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
		self.assertEquals(self.callback_result, False)
		self.assertNotEqual(self.callback_message, '')
		client = ZmqPubControlClientTestClass('uri', 'push_uri',
				'pub_uri', True, True, 'callback', 'context')
		client.raise_during_send = True
		client.discover_called = False
		client._push_sock = ZmqSocketTestClass()
		with self.assertRaises(ValueError):
			client._publish('channel', Item(TestFormatSubClass()), True,
					self.callback)

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
		self.assertTrue(client.verify_not_closed_called)
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
		self.assertEquals(client._pub_controller.connect_uri,
                'pub_uri'.encode('utf-8'))

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
		client._send_to_zmq({'content'.encode('utf-8'):
				'content'.encode('utf-8')}, 'chan'.encode('utf-8'))
		self.assertEquals(client._push_sock.send_data, tnetstring.dumps(
				{'content'.encode('utf-8'): 'content'.encode('utf-8'),
				'channel'.encode('utf-8'): 'chan'.encode('utf-8')}))
		client = ZmqPubControlClientTestClass2('uri')
		client._pub_controller = ZmqPubControllerTestClass()
		client._send_to_zmq({'content'.encode('utf-8'):
				'content'.encode('utf-8')}, 'chan'.encode('utf-8'))
		self.assertEquals(client._pub_controller.publish_channel,
				'chan'.encode('utf-8'))
		self.assertEquals(client._pub_controller.publish_content,
				tnetstring.dumps({'content'.encode('utf-8'):
				'content'.encode('utf-8')}))

	def test_verify_not_closed(self):
		client = ZmqPubControlClientTestClass3('uri')
		client._verify_not_closed()
		client.closed = True
		with self.assertRaises(ValueError):
			client._verify_not_closed()

	def test_discover_uris(self):
		global pollin_response
		global pollout_response
		global discovery_result
		global failed_discovery_result1
		global failed_discovery_result2
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
				tnetstring.dumps({'method'.encode('utf-8'):
				'get-zmq-uris'.encode('utf-8')}))
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
		client._discover_uris_async()
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
		client._context = ZmqContextTestClass()
		discovery_result = failed_discovery_result1
		client.resp = False
		with self.assertRaises(ValueError):
			client._discover_uris()
		self.assertTrue(client._context.last_socket_created.closed)
		client = ZmqPubControlClientTestClass4('tcp://localhost:5563',
				'push_uri', 'pub_uri')
		client.pub_uri = None
		client.push_uri = None
		client._context = ZmqContextTestClass()
		discovery_result = failed_discovery_result2
		client.resp = False
		with self.assertRaises(ValueError):
			client._discover_uris()
		self.assertTrue(client._context.last_socket_created.closed)

	def discovery_callback(self, push_uri, pub_uri, require_subscribers):
		self.discovery_push_uri = push_uri
		self.discovery_pub_uri = pub_uri
		self.discovery_require_subscribers = require_subscribers

	def test_end_discovery(self):
		client = ZmqPubControlClientTestClass4('tcp://localhost:5563',
				'push_uri', 'pub_uri')
		client._discovery_callback = self.discovery_callback
		client._discovery_completed = False
		client._end_discovery(True)
		self.assertTrue(client._discovery_completed)
		self.assertTrue(client.connect_called)
		self.assertEqual(self.discovery_push_uri, 'push_uri')
		self.assertEqual(self.discovery_pub_uri, 'pub_uri')
		self.assertEqual(self.discovery_require_subscribers, False)
		client = ZmqPubControlClientTestClass4('tcp://localhost:5563',
				'push_uri', 'pub_uri')
		client._discovery_completed = False
		client.connect_called = False
		client._end_discovery(False)
		self.assertFalse(client._discovery_completed)
		self.assertFalse(client.connect_called)

	def test_cleanup_discovery(self):
		client = ZmqPubControlClientTestClass4('tcp://localhost:5563',
				'push_uri', 'pub_uri')
		client._thread_cond.acquire()
		client._discovery_in_progress = True
		client._cleanup_discovery()
		self.assertFalse(client._discovery_in_progress)

	def test_set_discovered_uris(self):
		client = ZmqPubControlClientTestClass5('uri',
				'push_uri', 'pub_uri')
		result = {'publish-pull'.encode('utf-8'): 'push'.encode('utf-8'),
				'publish-sub'.encode('utf-8'): 'pub'.encode('utf-8')}
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

	def test_verify_discovered_uris(self):
		client = ZmqPubControlClientTestClass5('uri',
				'push_uri', 'pub_uri')
		client.push_uri = 'push'
		client.pub_uri = 'pub'
		client._verify_discovered_uris()
		client.push_uri = None
		client.pub_uri = 'pub'
		client._verify_discovered_uris()
		client.push_uri = 'push'
		client.pub_uri = None
		client._verify_discovered_uris()
		client.push_uri = None
		client.pub_uri = None
		with self.assertRaises(ValueError):
			client._verify_discovered_uris()

	def test_discovery_required_for_pub(self):
		client = ZmqPubControlClientTestClass4('uri',
				'push_uri', 'pub_uri')
		client.pub_uri = None
		client._require_subscribers = True
		client._discovery_completed = False
		self.assertTrue(client._discovery_required_for_pub())
		client.pub_uri = 'pub'
		self.assertFalse(client._discovery_required_for_pub())
		client.pub_uri = None
		client._require_subscribers = False
		self.assertFalse(client._discovery_required_for_pub())
		client._require_subscribers = True
		client._discovery_completed = True
		self.assertFalse(client._discovery_required_for_pub())

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

	def test_discovery_required_for_pub(self):
		client = ZmqPubControlClientTestClass('uri')
		client.pub_uri = None
		client._require_subscribers = True
		client._discovery_completed = False
		self.assertTrue(client._discovery_required_for_pub())
		client.pub_uri = 'uri'
		self.assertFalse(client._discovery_required_for_pub())
		client.pub_uri = None
		client._require_subscribers = False
		self.assertFalse(client._discovery_required_for_pub())
		client._require_subscribers = True
		client._discovery_completed = True
		self.assertFalse(client._discovery_required_for_pub())

if __name__ == '__main__':
		unittest.main()
