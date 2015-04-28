import sys
import unittest
sys.path.append('../')
from src.utilities import _ensure_utf8, _ensure_unicode

class TestUtilities(unittest.TestCase):
	def test_ensure_utf8(self):
		text = 'text'
		encoded_text = _ensure_utf8(text)		
		is_encoded = False
		try:
			if not isinstance(encoded_text, unicode):
				is_encoded = True
		except NameError:
			if not isinstance(encoded_text, str):
				is_encoded = True
		self.assertEqual(encoded_text, 'text'.encode('utf-8'))

	def test_ensure_unicode(self):
		text = 'text'.encode('utf-8')
		decoded_text = _ensure_unicode(text)		
		is_decoded = False
		try:
			if isinstance(decoded_text, unicode):
				is_decoded = True
		except NameError:
			if isinstance(decoded_text, str):
				is_decoded = True
		self.assertEqual(decoded_text, 'text')

if __name__ == '__main__':
		unittest.main()
