#    pcccbhandler.py
#    ~~~~~~~~~
#    This module implements the PubControlClientCallbackHandler class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

# The PubControlClientCallbackHandler class is used internally for allowing
# an async publish call made from the PubControl class to execute a callback
# method only a single time. A PubControl instance can potentially contain
# many PubControlClient instances in which case this class tracks the number
# of successful publishes relative to the total number of PubControlClient
# instances. A failure to publish in any of the PubControlClient instances
# will result in a failed result passed to the callback method and the error
# from the first encountered failure.
class PubControlClientCallbackHandler(object):

	# The initialize method accepts: a num_calls parameter which is an integer
	# representing the number of PubControlClient instances, and a callback
	# method to be executed after all publishing is complete.
	def __init__(self, num_calls, callback):
		self.num_calls = num_calls
		self.callback = callback
		self.success = True
		self.first_error_message = None

	# The handler method which is executed by PubControlClient when publishing
	# is complete. This method tracks the number of publishes performed and 
	# when all publishes are complete it will call the callback method
	# originally specified by the consumer. If publishing failures are
	# encountered only the first error is saved and reported to the callback
	# method.
	def handler(self, success, message):
		if not success and self.success:
			self.success = False
			self.first_error_message = message

		self.num_calls -= 1
		if self.num_calls <= 0:
			self.callback(self.success, self.first_error_message)
