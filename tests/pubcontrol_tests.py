import sys
import unittest
from base64 import b64encode, b64decode

try:
	import urllib.request as urllib2
except ImportError:
	import urllib2

sys.path.append('../')
from src.pubcontrol import PubControl
from src.pubcontrolclient import PubControlClient
from src.item import Item
from src.format import Format

class PubControlClientTestClass(object):
	def initialize(self):
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

class TestPubControl(unittest.TestCase):
	def test_initialize(self):
		pc = PubControl()
		self.assertEqual(len(pc.clients), 0)
		config = {'uri': 'uri', 'iss': 'iss', 'key': 'key'}
		pc = PubControl(config)
		self.assertEqual(len(pc.clients), 1)
		config = [{'uri': 'uri', 'iss': 'iss', 'key': 'key'},
				{'uri': 'uri', 'iss': 'iss', 'key': 'key'}]
		pc = PubControl(config)
		self.assertEqual(len(pc.clients), 2)

	def test_remove_all_clients(self):
		pc = PubControl()
		pc.clients.append('client')
		pc.remove_all_clients()
		self.assertEqual(len(pc.clients), 0)

	def test_add_client(self):
		pc = PubControl()
		pc.add_client('client')
		self.assertEqual(pc.clients[0], 'client')

	def test_apply_config(self):
		pc = PubControl()
		config = {'uri': 'uri'}
		pc.apply_config(config)
		self.assertEqual(pc.clients[0].uri, 'uri')
		pc = PubControl()
		config = [{'uri': 'uri'},
				{'uri': 'uri1', 'iss': 'iss1', 'key': 'key1'},
				{'uri': 'uri2', 'iss': 'iss2', 'key': 'key2'}]
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

if __name__ == '__main__':
		unittest.main()
