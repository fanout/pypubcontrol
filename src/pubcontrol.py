#    pubcontrol.py
#    ~~~~~~~~~
#    This module implements the PubControl class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

from .pcccbhandler import PubControlClientCallbackHandler
from .pubcontrolclient import PubControlClient

class PubControl(object):
	def __init__(self, config=None):
		self.clients = list()
		if config:
			self.apply_config(config)

	def remove_all_clients(self):
		self.clients = list()

	def add_client(self, client):
		self.clients.append(client)

	def apply_config(self, config):
		if not isinstance(config, list):
			config = [config]
		for entry in config:
			client = PubControlClient(entry['uri'])
			if 'iss' in entry:
				client.set_auth_jwt({'iss': entry['iss']}, entry['key'])

			self.clients.append(client)

	def publish(self, channel, item, blocking=False, callback=None):
		if blocking:
			for client in self.clients:
				client.publish(channel, item, blocking=True)
		else:
			if callback is not None:
				cb = PubControlClientCallbackHandler(len(self.clients), callback).handler
			else:
				cb = None

			for client in self.clients:
				client.publish(channel, item, blocking=False, callback=cb)

	def finish(self):
		for client in self.clients:
			client.finish()
