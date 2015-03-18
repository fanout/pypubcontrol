#    pubcontrol.py
#    ~~~~~~~~~
#    This module implements the PubControl class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

from .pcccbhandler import PubControlClientCallbackHandler
from .pubcontrolclient import PubControlClient

# The PubControl class allows a consumer to manage a set of publishing
# endpoints and to publish to all of those endpoints via a single publish
# or publish_async method call. A PubControl instance can be configured
# either using a hash or array of hashes containing configuration information
# or by manually adding PubControlClient instances.
class PubControl(object):

	# Initialize with or without a configuration. A configuration can be applied
	# after initialization via the apply_config method.
	def __init__(self, config=None):
		self.clients = list()
		if config:
			self.apply_config(config)

	# Remove all of the configured PubControlClient instances.
	def remove_all_clients(self):
		self.clients = list()

	# Add the specified PubControlClient instance.
	def add_client(self, client):
		self.clients.append(client)

	# Apply the specified configuration to this PubControl instance. The
	# configuration object can either be a hash or an array of hashes where
	# each hash corresponds to a single PubControlClient instance. Each hash
	# will be parsed and a PubControlClient will be created either using just
	# a URI or a URI and JWT authentication information.
	def apply_config(self, config):
		if not isinstance(config, list):
			config = [config]
		for entry in config:
			client = PubControlClient(entry['uri'])
			if 'iss' in entry:
				client.set_auth_jwt({'iss': entry['iss']}, entry['key'])

			self.clients.append(client)

	# The publish method for publishing the specified item to the specified
	# channel on the configured endpoint. The blocking parameter indicates
	# whether the call should be blocking or non-blocking. The callback method
	# is optional and will be passed the publishing results after publishing is
	# complete. Note that a failure to publish in any of the configured
	# PubControlClient instances will result in a failure result being passed
	# to the callback method along with the first encountered error message.
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

	# The finish method is a blocking method that ensures that all asynchronous
	# publishing is complete for all of the configured PubControlClient
	# instances prior to returning and allowing the consumer to proceed.
	def finish(self):
		for client in self.clients:
			client.finish()
