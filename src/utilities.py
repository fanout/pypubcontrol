#    utilities.py
#    ~~~~~~~~~
#    This module implements the utility methods.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

try:
	import zmq
except ImportError:
	zmq = None

try:
	import tnetstring
except ImportError:
	tnetstring = None

# An internal method to verify that the zmq and tnetstring packages are
# available. If not an exception is raised.
def _verify_zmq():
	if zmq is None:
		raise ValueError('zmq package must be installed')
	if tnetstring is None:
		raise ValueError('tnetstring package must be installed')

# An internal method for encoding the specified value as UTF8 only
# if it is unicode.
def _ensure_utf8(value):
	try:
		if isinstance(value, unicode):
			return value.encode('utf-8')
	except NameError:
		if isinstance(value, str):
			return value.encode('utf-8')
	return value

# An internal method for decoding the specified value as UTF8 only
# if it is binary.
def _ensure_unicode(value):
	try:
		if not isinstance(value, unicode):
			return value.decode('utf-8')
	except NameError:
		if not isinstance(value, str):
			return value.decode('utf-8')
	return value
