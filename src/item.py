#    item.py
#    ~~~~~~~~~
#    This module implements the Item class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

from .format import Format
from .utilities import _ensure_utf8, _ensure_unicode

# The Item class is a container used to contain one or more format
# implementation instances where each implementation instance is of a
# different type of format. An Item instance may not contain multiple
# implementations of the same type of format. An Item instance is then
# serialized into a hash that is used for publishing to clients.
class Item(object):

	# The initialize method can accept either a single Format implementation
	# instance or an array of Format implementation instances. Optionally
	# specify an ID and/or previous ID to be sent as part of the message
	# published to the client.
	def __init__(self, formats, id=None, prev_id=None):
		self.id = id
		self.prev_id = prev_id
		if isinstance(formats, Format):
			formats = [formats]
		self.formats = formats

	# The export method serializes all of the formats, ID, and previous ID
	# into a hash that is used for publishing to clients. If more than one
	# instance of the same type of Format implementation was specified then
	# an error will be raised. If the formats_field parameter is set to true
	# then the formats will be exported within their own 'formats' key. If
	# the tnetstring parameter is set to true then ID and previous ID are
	# encoded as UTF-8, whereas if it is set to false then ID and previous ID
	# are decoded as unicode.
	def export(self, formats_field=False, tnetstring=False):
		format_types = []
		for format in self.formats:
			if format.__class__.__name__ in format_types:
				raise ValueError('more than one instance of ' +
						format.__class__.__name__ + ' specified')
			format_types.append(format.__class__.__name__)
		out = dict()
		if self.id:
			if tnetstring:
				out['id'] = _ensure_utf8(self.id)
			else:
				out['id'] = _ensure_unicode(self.id)
		if self.prev_id:
			if tnetstring:
				out['prev-id'] = _ensure_utf8(self.prev_id)
			else:
				out['prev-id'] = _ensure_unicode(self.prev_id)
		if formats_field:
			out['formats'] = dict()
			for f in self.formats:
				if tnetstring:
					out['formats'][f.name()] = f.export(tnetstring)
				else:
					out['formats'][f.name()] = f.export()
		else:
			for f in self.formats:
				if tnetstring:
					out[f.name()] = f.export(tnetstring)
				else:
					out[f.name()] = f.export()
		return out
