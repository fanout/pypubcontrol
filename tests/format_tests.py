import sys
import unittest

sys.path.append('../')
from src.format import Format

class TestFormatSubClass(Format):
	def name(self):
		return 'test_name'

	def export(self):
		return 'test_export'

class TestFormatSubClass2(Format):
	def name(self):
		return 'test_name'

	def export(self, tnetstring=False):
		return 'test_export'

class TestFormat(unittest.TestCase):
	def test_inheritance(self):
		format = Format()
		format.name()
		format.export()
		subclass = TestFormatSubClass()
		self.assertEqual(subclass.name(), 'test_name')
		self.assertEqual(subclass.export(), 'test_export')
		subclass = TestFormatSubClass2()
		self.assertEqual(subclass.name(), 'test_name')
		self.assertEqual(subclass.export(), 'test_export')

if __name__ == '__main__':
		unittest.main()
