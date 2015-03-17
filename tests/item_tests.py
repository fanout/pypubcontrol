import sys
import unittest

sys.path.append('../')
from src.format import Format
from src.item import Item

class TestFormatSubClass(Format):
	def name(self):
		return 'name'

	def export(self):
		return {'body': 'bodyvalue'}

class TestFormatSubClass2(Format):
	def name(self):
		return 'name'

	def export(self):
		return {'body': 'bodyvalue'}

class TestItem(unittest.TestCase):
	def test_initialize(self):
		item = Item([0, 'format'], 'id', 'prev-id')
		self.assertEqual(item.id, 'id');
		self.assertEqual(item.prev_id, 'prev-id');
		self.assertEqual(item.formats, [0, 'format']);
		format = TestFormatSubClass()
		format2 = TestFormatSubClass2()
		item = Item([format, format2])
		self.assertEqual(item.formats, [format, format2]);
		item = Item(format)
		self.assertEqual(item.formats, [format]);

	def test_export(self):
		format = TestFormatSubClass()
		out =	Item(format, 'id', 'prev-id').export()
		self.assertEqual(out['name'], { 'body': 'bodyvalue' })
		self.assertEqual(out['id'], 'id')
		self.assertEqual(out['prev-id'], 'prev-id')
		out = Item(format).export()
		self.assertFalse('id' in out)
		self.assertFalse('prev-id' in out)

	def test_export_same_format_type(self):
		item = Item([TestFormatSubClass(), TestFormatSubClass()])
		with self.assertRaises(ValueError):
				item.export()

if __name__ == '__main__':
		unittest.main()
