#    item.py
#    ~~~~~~~~~
#    This module implements the Item class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

from .format import Format

class Item(object):
	def __init__(self, formats, id=None, prev_id=None):
		self.id = id
		self.prev_id = prev_id
		if isinstance(formats, Format):
			formats = [formats]
		self.formats = formats

	def export(self):
		out = dict()
		if self.id:
			out['id'] = self.id
		if self.prev_id:
			out['prev-id'] = self.prev_id
		for f in self.formats:
			out[f.name()] = f.export()
		return out
