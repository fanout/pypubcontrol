#    format.py
#    ~~~~~~~~~
#    This module implements the Format class.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

# The Format class is provided as a base class for all publishing
# formats that are included in the Item class. Examples of format
# implementations include JsonObjectFormat and HttpStreamFormat.
class Format(object):

	# The name of the format which should return a string. Examples
	# include 'json-object' and 'http-response'
	def name(self):
		pass

	# The export method which should return a format-specific hash
	# containing the required format-specific data.
	def export(self):
		pass
