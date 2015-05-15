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
	def close(self):
		self.closed = True

class TestFormatSubClass(Format):
	def name(self):
		return 'name'.encode('utf-8')

	def export(self):
		return {'body': 'bodyvalue'}

class PubControlTestClass(PubControl):
	def close(self):
		self.close_called = True

	def _verify_not_closed(self):
		self.verify_not_closed_called = True

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

	def wait_all_sent(self):
		self.wait_all_sent_called = True

	def close(self):
		self.close_called = True

	def publish(self, channel, item, blocking=False, callback=None):
		self.publish_channel = channel
		self.publish_item = item
		self.publish_blocking = blocking
		self.publish_callback = callback

class ZmqPubControlClientTestClass():
	def close(self):
		self.closed = True

class ZmqPubControlClientTestClass2():
	def __init__(self, uri, push_uri=None, pub_uri=None,
			require_subscribers=False, disable_pub=False, sub_callback=None, 
			zmq_context=None):
		self.uri = uri
		self.push_uri = push_uri
		self.pub_uri = pub_uri
		self.require_subscribers = require_subscribers
		self.disable_pub = disable_pub
		self.sub_callback = sub_callback
		self.zmq_context = zmq_context

class ThreadTestClass():
	def join(self):
		self.join_called = True

class ZmqPubControllerTestClass():
	def __init__(self, callback=None, context=None):
		self.callback = callback
		self.context = context
		self._thread = ThreadTestClass()
		self.subscriptions = list()

	def publish(self, channel, content):
		self.publish_channel = channel
		self.publish_content = content

	def connect(self, uri):
		self.connect_uri = uri

	def disconnect(self, uri):
		self.disconnect_uri = uri

	def stop(self):
		self.stop_called = True

class TestPubControl(unittest.TestCase):
	def test_close_pubcontrols(self):
		pubcontroltest._zmqpubcontrols = list()
		pub1 = PubControl()
		pub2 = PubControl()
		pubcontroltest._zmqpubcontrols.append(pub1)
		pubcontroltest._zmqpubcontrols.append(pub2)
		pubcontroltest._close_pubcontrols()
		self.assertTrue(pub1.closed)
		self.assertTrue(pub2.closed)

	def test_initialize(self):
		pc = PubControl()
		self.assertEqual(len(pc.clients), 0)
		self.assertEqual(pc._sub_callback, None)
		self.assertEqual(pc._zmq_pub_controller, None)
		self.assertNotEqual(pc._zmq_ctx, None)
		self.assertTrue(pc in pubcontroltest._pubcontrols)
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
		self.assertEqual(pc._zmq_ctx, zmq.Context.instance())

	def test_add_client(self):
		pc = PubControlTestClass()
		pc.add_client('client')
		self.assertTrue(pc.verify_not_closed_called)
		self.assertEqual(pc.clients[0], 'client')

	def test_finish(self):
		pc = PubControlTestClass()
		pccs = []
		for n in range(0, 3):
			pcc = PubControlClientTestClass()
			pccs.append(pcc)
			pc.add_client(pcc)
		pc.finish()
		self.assertTrue(pc.verify_not_closed_called)
		for n in range(0, 3):
			self.assertTrue(pccs[n].wait_all_sent_called)

	def test_wait_all_sent(self):
		pc = PubControlTestClass()
		pccs = []
		for n in range(0, 3):
			pcc = PubControlClientTestClass()
			pccs.append(pcc)
			pc.add_client(pcc)
		pc.wait_all_sent()
		self.assertTrue(pc.verify_not_closed_called)
		for n in range(0, 3):
			self.assertTrue(pccs[n].wait_all_sent_called)

	def test_apply_config(self):
		pc = PubControlTestClass()
		config = {'uri': 'uri'}
		pc.apply_config(config)
		self.assertTrue(pc.verify_not_closed_called)
		self.assertEqual(pc.clients[0].uri, 'uri')
		pc = PubControlTestClass()
		pubcontroltest.ZmqPubControlClient = ZmqPubControlClientTestClass2
		config = [{'uri': 'uri'},
				{'uri': 'uri1', 'iss': 'iss1', 'key': 'key1'},
				{'uri': 'uri2', 'iss': 'iss2', 'key': 'key2'},
				{'zmq_uri': 'zmq_uri'},
				{'zmq_push_uri': 'push_uri'},
				{'zmq_pub_uri': 'pub_uri', 'require_subscribers': True },
				{'zmq_uri': 'zmq_uri2', 'zmq_push_uri': 'push_uri2', 
						'zmq_pub_uri': 'pub_uri2'},
				{'zmq_uri': 'zmq_uri3', 'zmq_push_uri': 'push_uri3', 
						'zmq_pub_uri': 'pub_uri3',
						'require_subscribers': True}]
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
		self.assertEqual(pc.clients[3].pub_uri, None)
		self.assertTrue(pc.clients[3].disable_pub)
		self.assertEqual(pc.clients[3].require_subscribers, False)
		self.assertEqual(pc.clients[3].sub_callback, None)
		self.assertEqual(pc.clients[3].zmq_context, pc._zmq_ctx)
		self.assertEqual(pc.clients[4].push_uri, 'push_uri')
		self.assertTrue(pc.clients[4].disable_pub)
		self.assertEqual(pc.clients[4].require_subscribers, False)
		self.assertEqual(pc.clients[4].sub_callback, None)
		self.assertEqual(pc.clients[4].zmq_context, pc._zmq_ctx)
		self.assertEqual(pc.clients[5].pub_uri, 'pub_uri')
		self.assertTrue(pc.clients[5].disable_pub)
		self.assertEqual(pc.clients[5].require_subscribers, True)
		self.assertEqual(pc.clients[5].sub_callback, None)
		self.assertEqual(pc.clients[5].zmq_context, pc._zmq_ctx)
		self.assertEqual(pc.clients[6].uri, 'zmq_uri2')
		self.assertEqual(pc.clients[6].push_uri, 'push_uri2')
		self.assertEqual(pc.clients[6].pub_uri, 'pub_uri2')
		self.assertTrue(pc.clients[6].disable_pub)
		self.assertEqual(pc.clients[6].require_subscribers, False)
		self.assertEqual(pc.clients[6].sub_callback, None)
		self.assertEqual(pc.clients[6].zmq_context, pc._zmq_ctx)
		self.assertEqual(pc.clients[7].uri, 'zmq_uri3')
		self.assertEqual(pc.clients[7].push_uri, 'push_uri3')
		self.assertEqual(pc.clients[7].pub_uri, 'pub_uri3')
		self.assertTrue(pc.clients[7].disable_pub)
		self.assertEqual(pc.clients[7].require_subscribers, True)
		self.assertEqual(pc.clients[7].sub_callback, None)
		self.assertEqual(pc.clients[7].zmq_context, pc._zmq_ctx)
		self.assertEqual(len(pc.connect_uris), 2)
		self.assertEqual(pc.connect_uris[0], 'pub_uri')
		self.assertEqual(pc.connect_uris[1], 'pub_uri3')

	def test_publish_blocking(self):
		pc = PubControlTestClass()
		pccs = []
		for n in range(0, 3):
			pcc = PubControlClientTestClass()
			pccs.append(pcc)
			pc.add_client(pcc)
		pc.publish('channel', 'item', True)
		self.assertTrue(pc.verify_not_closed_called)
		for n in range(0, 3):
			self.assertEqual(pccs[n].publish_channel, 'channel')
			self.assertEqual(pccs[n].publish_item, 'item')
			self.assertEqual(pccs[n].publish_blocking, True)

	def test_publish_send_to_zmq_test(self):
		pc = PubControlTestClass()
		pc.publish('chan', 'item')
		self.assertEqual(pc.channel, 'chan')
		self.assertEqual(pc.item, 'item')

	def test_send_to_zmq(self):
		pc = PubControl()
		pc._zmq_pub_controller = ZmqPubControllerTestClass()
		pc._send_to_zmq('chan', Item(TestFormatSubClass()))
		fmt = TestFormatSubClass()
		pc._send_to_zmq('chan', Item(fmt))
		self.assertEqual(pc._zmq_pub_controller.publish_channel,
				'chan'.encode('utf-8'))
		is_encoded = False
		try:
			if not isinstance(pc._zmq_pub_controller.publish_channel, unicode):
				is_encoded = True
		except NameError:
			if not isinstance(pc._zmq_pub_controller.publish_channel, str):
				is_encoded = True

	def test_pub_controller_callback(self):
		pc = PubControl()
		pc._sub_callback = self.sub_callback_for_testing
		pc._zmq_pub_controller = ZmqPubControllerTestClass()
		self.clear_sub_callback_for_testing()
		pc._pub_controller_callback('event_type', 'chan')
		self.assertTrue(self.sub_callback_executed)
		self.assertEqual(self.sub_chan, 'chan')
		self.assertEqual(self.sub_event_type, 'event_type')
		self.clear_sub_callback_for_testing()
		pc._zmq_pub_controller.subscriptions.append('chan')
		pc._pub_controller_callback('event_type', 'chan')
		self.assertFalse(self.sub_callback_executed)
		client = ClientTestClass()
		client._zmq_pub_controller = ZmqPubControllerTestClass()
		client._zmq_pub_controller.subscriptions.append('chan2')
		pc.add_client(client)
		pc._pub_controller_callback('event_type', 'chan2')
		self.assertFalse(self.sub_callback_executed)

	def sub_callback_for_testing(self, event_type, chan):
		self.sub_event_type = event_type
		self.sub_chan = chan
		self.sub_callback_executed = True

	def clear_sub_callback_for_testing(self):
		self.sub_eventType = None
		self.sub_chan = None
		self.sub_callback_executed = False

	def test_connect_zmq_pub_uri(self):
		pc = PubControl()
		pc._disconnect_zmq_pub_uri('uri')
		pc._zmq_pub_controller = ZmqPubControllerTestClass()
		pc._connect_zmq_pub_uri('connect')
		self.assertEqual(pc._zmq_pub_controller.connect_uri, 'connect')
		pc._disconnect_zmq_pub_uri('disconnect')
		self.assertEqual(pc._zmq_pub_controller.disconnect_uri, 'disconnect')

if __name__ == '__main__':
		unittest.main()
