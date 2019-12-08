import sys
import unittest
sys.path.append('../')
from src import utilities

class TestUtilities(unittest.TestCase):
	def test_verify_zmq(self):
		utilities._verify_zmq()
		utilities.zmq = None
		with self.assertRaises(ValueError):
				utilities._verify_zmq()
		utilities.zmq = 'zmq'
		utilities._verify_zmq()
		utilities.tnetstring = None
		with self.assertRaises(ValueError):
				utilities._verify_zmq()

	def test_ensure_utf8(self):
		text = 'text'
		encoded_text = utilities._ensure_utf8(text)		
		is_encoded = False
		self.assertTrue(self.is_encoded(encoded_text))
		self.assertEqual(encoded_text, 'text'.encode('utf-8'))
		data = { 'key1': 'val1', 'key2'.encode('utf-8'): ['val2', 
					{'key3': ['val3', 'val4'.encode('utf-8')]}] }
		encoded_data = utilities._ensure_utf8(data)
		for key in encoded_data:
			self.assertTrue(self.is_encoded(key))
			if key == 'key2':
				self.assertTrue(self.is_encoded(encoded_data[key][0]))
				self.assertTrue(self.is_encoded(encoded_data[key][1]))
			else:
				self.assertTrue(self.is_encoded(encoded_data[key]))

	def is_encoded(self, value):
		is_encoded = False
		try:
			if not isinstance(value, unicode):
				is_encoded = True
		except NameError:
			if not isinstance(value, str):
				is_encoded = True
		return is_encoded

	def test_ensure_unicode(self):
		text = 'text'.encode('utf-8')
		decoded_text = utilities._ensure_unicode(text)	
		self.assertEqual(decoded_text, 'text')
		data = { 'key1'.encode('utf-8'): 'val1'.encode('utf-8'),
				'key2'.encode('utf-8'): ['val2', 
				{'key3'.encode('utf-8'): ['val3', 'val4'.encode('utf-8')]}] }
		utilities._ensure_unicode(data)

if __name__ == '__main__':
	unittest.main()
