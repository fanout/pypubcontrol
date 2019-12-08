import sys
import unittest

sys.path.append('../')
from src.pcccbhandler import PubControlClientCallbackHandler

class TestPubControlClientCallbackHandler(unittest.TestCase):
	def callback(self, success, error):
		self.callback_error = error
		self.callback_success = success

	def setUp(self):
		self.callback_success = None
		self.callback_error = None

	def test_initialize(self):
		handler = PubControlClientCallbackHandler(1, self.callback)
		self.assertEqual(handler.num_calls, 1)
		self.assertEqual(handler.success, True)
		self.assertEqual(handler.first_error_message, None)
		self.assertEqual(handler.callback, self.callback)

	def test_handler_one_call_success(self):
		handler = PubControlClientCallbackHandler(1, self.callback)
		handler.handler(True, None)
		self.assertTrue(self.callback_success)
		self.assertTrue(self.callback_error == None)

	def test_handler_three_call_success(self):
		handler = PubControlClientCallbackHandler(3, self.callback)
		handler.handler(True, None)
		self.assertTrue(self.callback_success == None)
		handler.handler(True, None)
		self.assertTrue(self.callback_success == None)
		handler.handler(True, 'should not be there')
		self.assertTrue(self.callback_success)
		self.assertTrue(self.callback_error == None)

	def test_handler_one_call_failure(self):
		handler = PubControlClientCallbackHandler(1, self.callback)
		handler.handler(False, 'failure')
		self.assertTrue(self.callback_success == False)
		self.assertTrue(self.callback_error == 'failure')

	def test_handler_three_call_failure(self):
		handler = PubControlClientCallbackHandler(3, self.callback)
		handler.handler(False, 'failure')
		self.assertTrue(self.callback_success == None)
		self.assertTrue(self.callback_error == None)
		handler.handler(True, None)
		self.assertTrue(self.callback_success == None)
		self.assertTrue(self.callback_error == None)
		handler.handler(False, 'failure2')
		self.assertTrue(self.callback_success == False)
		self.assertTrue(self.callback_error == 'failure')

if __name__ == '__main__':
	unittest.main()
