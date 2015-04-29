import zmq
import tnetstring
import sys
import unittest
from base64 import b64encode, b64decode

try:
	import urllib.request as urllib2
except ImportError:
	import urllib2

sys.path.append('../')
import src.pubcontrol as pubcontroltest
from src.pubcontrol import PubControl
from src.pubcontrolclient import PubControlClient
from src.item import Item
from src.format import Format

class ClientTestClass():
	pass

class TestSubMonitorClass():
	def __init__(self):
		self.subscriptions = list()

class TestFormatSubClass(Format):
	def name(self):
		return 'name'

	def export(self, tnetstring=False):
		self.tnetstring = True
		return {'body': 'bodyvalue'}

class PubControlTestClass(PubControl):
	def _send_to_zmq(self, channel, item):
		self.channel = channel
		self.item = item

	def _connect_zmq_pub_uri(self, uri):
		try:
			self.connect_uris.append(uri)
		except:
			self.connect_uris = list()
			self.connect_uris.append(uri)

class PubControlClientTestClass():
	def __init__(self):
		self.was_finish_called = False
		self.publish_channel = None
		self.publish_item = None
		self.publish_blocking = None
		self.publish_callback = None

	def finish(self):
		self.was_finish_called = True

	def publish(self, channel, item, blocking=False, callback=None):
		self.publish_channel = channel
		self.publish_item = item
		self.publish_blocking = blocking
		self.publish_callback = callback

class ZmqSocketTestClass():
	def close(self):
		self.closed = True

class ZmqSocketTestClass2():
	def __init__(self):
		self.closed = True

	def send_multipart(self, data):
		self.data = data

	def connect(self, uri):
		self.uri = uri

class ZmqContextTestClass():
	def socket(self, socket_type):
		self.socket_type = socket_type
		return ZmqSocketTestClass2()

class ZmqPubControlClientTestClass():
	def close(self):
		self.closed = True

class ZmqPubControlClientTestClass2():
	def __init__(self, uri, zmq_push_uri=None, zmq_pub_uri=None,
			require_subscribers=False, disable_pub=False, sub_callback=None, 
			zmq_context=None):
		self.uri = uri
		self.zmq_push_uri = zmq_push_uri
		self.zmq_pub_uri = zmq_pub_uri
		self.require_subscribers = require_subscribers
		self.disable_pub = disable_pub
		self.sub_callback = sub_callback
		self.zmq_context = zmq_context

class TestPubControl(unittest.TestCase):
	def test_initialize(self):
		pc = PubControl()
		self.assertEqual(len(pc.clients), 0)
		self.assertEqual(pc._sub_callback, None)
		self.assertEqual(pc._zmq_sub_monitor, None)
		self.assertEqual(pc._zmq_sock, None)
		self.assertNotEqual(pc._zmq_ctx, None)
		pc = PubControl(None, 'subcallback', 'zmqcontext')
		self.assertEqual(pc._sub_callback, 'subcallback')
		self.assertEqual(pc._zmq_ctx, 'zmqcontext')
		self.assertEqual(len(pc.clients), 0)
		config = {'uri': 'uri', 'iss': 'iss', 'key': 'key'}
		pc = PubControl(config)
		self.assertEqual(len(pc.clients), 1)
		config = [{'uri': 'uri', 'iss': 'iss', 'key': 'key'},
				{'uri': 'uri', 'iss': 'iss', 'key': 'key'}]
		pc = PubControl(config)
		self.assertEqual(len(pc.clients), 2)
		pubcontroltest.zmq = None
		pc = PubControl()
		self.assertEqual(pc._zmq_ctx, None)
		pubcontroltest.zmq = zmq

	def test_remove_all_clients(self):
		pc = PubControl()
		sock = ZmqSocketTestClass()
		pc._zmq_sock = sock
		pc.clients.append(PubControlClient('uri'))
		zmqclient = ZmqPubControlClientTestClass()
		pc.clients.append(zmqclient)
		pc.remove_all_clients()
		self.assertTrue(sock.closed)
		self.assertTrue(zmqclient)
		self.assertEqual(len(pc.clients), 0)
		self.assertEqual(pc._zmq_sock, None)

	def test_add_client(self):
		pc = PubControl()
		pc.add_client('client')
		self.assertEqual(pc.clients[0], 'client')

	def test_apply_config(self):
		pc = PubControl()
		config = {'uri': 'uri'}
		pc.apply_config(config)
		self.assertEqual(pc.clients[0].uri, 'uri')
		pc = PubControlTestClass()
		pubcontroltest.ZmqPubControlClient = ZmqPubControlClientTestClass2
		config = [{'uri': 'uri'},
				{'uri': 'uri1', 'iss': 'iss1', 'key': 'key1'},
				{'uri': 'uri2', 'iss': 'iss2', 'key': 'key2'},
				{'zmq_uri': 'zmq_uri'},
				{'zmq_push_uri': 'zmq_push_uri'},
				{'zmq_pub_uri': 'zmq_pub_uri', },
				{'zmq_uri': 'zmq_uri2', 'zmq_push_uri': 'zmq_push_uri2', 
						'zmq_pub_uri': 'zmq_pub_uri2'},
				{'zmq_uri': 'zmq_uri3', 'zmq_push_uri': 'zmq_push_uri3', 
						'zmq_pub_uri': 'zmq_pub_uri3',
						'zmq_require_subscribers': True}]
		pc.apply_config(config)
		self.assertEqual(pc.clients[0].uri, 'uri')
		self.assertEqual(pc.clients[0].auth_jwt_claim, None)
		self.assertEqual(pc.clients[0].auth_jwt_key, None)
		self.assertEqual(pc.clients[1].uri, 'uri1')
		self.assertEqual(pc.clients[1].auth_jwt_claim, {'iss': 'iss1'})
		self.assertEqual(pc.clients[1].auth_jwt_key, 'key1')
		self.assertEqual(pc.clients[2].uri, 'uri2')
		self.assertEqual(pc.clients[2].auth_jwt_claim, {'iss': 'iss2'})
		self.assertEqual(pc.clients[2].auth_jwt_key, 'key2')
		self.assertEqual(pc.clients[3].uri, 'zmq_uri')
		self.assertEqual(pc.clients[3].zmq_pub_uri, None)
		self.assertTrue(pc.clients[3].disable_pub)
		self.assertEqual(pc.clients[3].require_subscribers, False)
		self.assertEqual(pc.clients[3].sub_callback, None)
		self.assertEqual(pc.clients[3].zmq_context, pc._zmq_ctx)
		self.assertEqual(pc.clients[4].zmq_push_uri, 'zmq_push_uri')
		self.assertTrue(pc.clients[4].disable_pub)
		self.assertEqual(pc.clients[4].require_subscribers, False)
		self.assertEqual(pc.clients[4].sub_callback, None)
		self.assertEqual(pc.clients[4].zmq_context, pc._zmq_ctx)
		self.assertEqual(pc.clients[5].zmq_pub_uri, 'zmq_pub_uri')
		self.assertTrue(pc.clients[5].disable_pub)
		self.assertEqual(pc.clients[5].require_subscribers, False)
		self.assertEqual(pc.clients[5].sub_callback, None)
		self.assertEqual(pc.clients[5].zmq_context, pc._zmq_ctx)
		self.assertEqual(pc.clients[6].uri, 'zmq_uri2')
		self.assertEqual(pc.clients[6].zmq_push_uri, 'zmq_push_uri2')
		self.assertEqual(pc.clients[6].zmq_pub_uri, 'zmq_pub_uri2')
		self.assertTrue(pc.clients[6].disable_pub)
		self.assertEqual(pc.clients[6].require_subscribers, False)
		self.assertEqual(pc.clients[6].sub_callback, None)
		self.assertEqual(pc.clients[6].zmq_context, pc._zmq_ctx)
		self.assertEqual(pc.clients[7].uri, 'zmq_uri3')
		self.assertEqual(pc.clients[7].zmq_push_uri, 'zmq_push_uri3')
		self.assertEqual(pc.clients[7].zmq_pub_uri, 'zmq_pub_uri3')
		self.assertTrue(pc.clients[7].disable_pub)
		self.assertEqual(pc.clients[7].require_subscribers, True)
		self.assertEqual(pc.clients[7].sub_callback, None)
		self.assertEqual(pc.clients[7].zmq_context, pc._zmq_ctx)
		self.assertEqual(len(pc.connect_uris), 2)
		self.assertEqual(pc.connect_uris[0], 'zmq_pub_uri')
		self.assertEqual(pc.connect_uris[1], 'zmq_pub_uri3')

	def test_apply_config_exception1(self):
		pubcontroltest.zmq = None
		pc = pubcontroltest.PubControl()
		config = [{'zmq_uri': 'zmquri'}]
		with self.assertRaises(ValueError):
			pc.apply_config(config)
		pubcontroltest.zmq = zmq

	def test_apply_config_exception2(self):
		pubcontroltest.tnetstring = None
		pc = pubcontroltest.PubControl()
		config = [{'zmq_uri': 'zmquri'}]
		with self.assertRaises(ValueError):
			pc.apply_config(config)
		pubcontroltest.tnetstring = tnetstring

	def test_finish(self):
		pc = PubControl()
		pccs = []
		for n in range(0, 3):
			pcc = PubControlClientTestClass()
			pccs.append(pcc)
			pc.add_client(pcc)
		pc.finish()
		for n in range(0, 3):
			self.assertTrue(pccs[n].was_finish_called)

	def test_publish_blocking(self):
		pc = PubControl()
		pccs = []
		for n in range(0, 3):
			pcc = PubControlClientTestClass()
			pccs.append(pcc)
			pc.add_client(pcc)
		pc.publish('channel', 'item', True)
		for n in range(0, 3):
			self.assertEqual(pccs[n].publish_channel, 'channel')
			self.assertEqual(pccs[n].publish_item, 'item')
			self.assertEqual(pccs[n].publish_blocking, True)

	def test_publish_without_callback(self):
		pc = PubControl()
		pccs = []
		for n in range(0, 3):
			pcc = PubControlClientTestClass()
			pccs.append(pcc)
			pc.add_client(pcc)
		pc.publish('channel', 'item')
		for n in range(0, 3):
			self.assertEqual(pccs[n].publish_channel, 'channel')
			self.assertEqual(pccs[n].publish_item, 'item')
			self.assertEqual(pccs[n].publish_callback, None)
			self.assertEqual(pccs[n].publish_blocking, False)

	def test_publish_with_callback(self):
		self.has_callback_been_called = False
		pc = PubControl()
		pccs = []
		for n in range(0, 3):
			pcc = PubControlClientTestClass()
			pccs.append(pcc)
			pc.add_client(pcc)
		pc.publish('channel', 'item', False, self.callback_for_testing)
		for n in range(0, 3):
			self.assertEqual(pccs[n].publish_channel, 'channel')
			self.assertEqual(pccs[n].publish_item, 'item')
			self.assertEqual(pccs[n].publish_blocking, False)
			pccs[n].publish_callback(False, 'error')
		self.assertTrue(self.has_callback_been_called)

	def callback_for_testing(self, result, error):
		self.assertEqual(self.has_callback_been_called, False)
		self.assertEqual(result, False)
		self.assertEqual(error, 'error')
		self.has_callback_been_called = True

	def test_publish_send_to_zmq_test(self):
		pc = PubControlTestClass()
		pc.publish('chan', 'item')
		self.assertEqual(pc.channel, 'chan')
		self.assertEqual(pc.item, 'item')

	def test_send_to_zmq(self):
		pc = PubControl()
		pc._send_to_zmq('chan', Item(TestFormatSubClass()))
		pc._zmq_sock = ZmqSocketTestClass2()
		fmt = TestFormatSubClass()
		pc._send_to_zmq('chan', Item(fmt))
		self.assertTrue(fmt.tnetstring)
		self.assertEqual(pc._zmq_sock.data[0], 'chan')
		is_encoded = False
		try:
			if not isinstance(pc._zmq_sock.data[0], unicode):
				is_encoded = True
		except NameError:
			if not isinstance(pc._zmq_sock.data[0], str):
				is_encoded = True
		self.assertEqual(pc._zmq_sock.data[0], 'chan'.encode('utf-8'))
		self.assertTrue(is_encoded)
		self.assertEqual(pc._zmq_sock.data[1],
				tnetstring.dumps(
				Item(TestFormatSubClass()).export(True, True)))

	def test_connect_to_zmq(self):
		pc = PubControl()
		pc._zmq_ctx = ZmqContextTestClass()
		pc._sub_callback = 'callback'
		pc._connect_zmq_pub_uri('uri')
		self.assertEqual(pc._zmq_sock.linger, 0)
		self.assertEqual(pc._zmq_sock.uri, 'uri')
		self.assertEqual(pc._zmq_sub_monitor._socket, pc._zmq_sock)
		self.assertEqual(pc._zmq_sub_monitor._lock, pc._lock)
		self.assertEqual(pc._zmq_sub_monitor._callback, pc._sub_callback)
		self.assertEqual(pc._zmq_ctx.socket_type, zmq.XPUB)

	def test_submonitor_callback(self):
		pc = PubControl()
		pc._sub_callback = self.sub_callback_for_testing
		pc._sub_monitor = TestSubMonitorClass()
		self.clear_sub_callback_for_testing()
		pc._submonitor_callback('eventType', 'chan')
		self.assertTrue(self.sub_callback_executed)
		self.assertEqual(self.sub_chan, 'chan')
		self.assertEqual(self.sub_eventType, 'eventType')
		self.clear_sub_callback_for_testing()
		pc._sub_monitor.subscriptions.append('chan')
		pc._submonitor_callback('eventType', 'chan')
		self.assertFalse(self.sub_callback_executed)
		client = ClientTestClass()
		client._sub_monitor = TestSubMonitorClass()
		client._sub_monitor.subscriptions.append('chan2')
		pc.add_client(client)
		pc._submonitor_callback('eventType', 'chan2')
		self.assertFalse(self.sub_callback_executed)

	def sub_callback_for_testing(self, eventType, chan):
		self.sub_eventType = eventType
		self.sub_chan = chan
		self.sub_callback_executed = True

	def clear_sub_callback_for_testing(self):
		self.sub_ventType = None
		self.sub_chan = None
		self.sub_callback_executed = False

if __name__ == '__main__':
		unittest.main()
