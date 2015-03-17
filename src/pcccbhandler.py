#    pcccbhandler.py
#    ~~~~~~~~~
#    This module implements the PubControlClientCallbackHandler class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

class PubControlClientCallbackHandler(object):
	def __init__(self, num_calls, callback):
		self.num_calls = num_calls
		self.callback = callback
		self.success = True
		self.first_error_message = None

	def handler(self, success, message):
		if not success and self.success:
			self.success = False
			self.first_error_message = message

		self.num_calls -= 1
		if self.num_calls <= 0:
			self.callback(self.success, self.first_error_message)
