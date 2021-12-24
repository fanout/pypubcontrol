#    utilities.py
#    ~~~~~~~~~
#    This module implements the utility methods.
#    :authors: Justin Karneges, Konstantin Bokarius.
#    :copyright: (c) 2015 by Fanout, Inc.
#    :license: MIT, see LICENSE for more details.

import sys
import jwt
import calendar
import copy
from datetime import datetime

try:
	import zmq
except ImportError:
	zmq = None

try:
	import tnetstring
except ImportError:
	tnetstring = None

is_python3 = sys.version_info >= (3,)

if is_python3:
	import collections.abc as collections
else:
	import collections

# An internal method to verify that the zmq and tnetstring packages are
# available. If not an exception is raised.
def _verify_zmq():
	if zmq is None:
		raise ValueError('zmq package must be installed')
	if tnetstring is None:
		raise ValueError('tnetstring package must be installed')

# An internal method for encoding the specified value as UTF8 only
# if it is unicode. This method acts recursively and will process nested
# lists and dicts.
def _ensure_utf8(value):
	if is_python3:
		if isinstance(value, str):
			return value.encode('utf-8')
	else:
		if isinstance(value, unicode):
			return value.encode('utf-8')
		elif isinstance(value, str):
			return value
	if isinstance(value, collections.Mapping):
		return dict(map(_ensure_utf8, value.items()))
	elif isinstance(value, collections.Iterable):
		return type(value)(map(_ensure_utf8, value))
	return value

# An internal method for decoding the specified value as UTF8 only
# if it is binary. This method acts recursively and will process nested
# lists and dicts.
def _ensure_unicode(value):
	if is_python3:
		if isinstance(value, bytes):
			return value.decode('utf-8')
		if isinstance(value, str):
			return value
	else:
		if isinstance(value, str):
			return value.decode('utf-8')
		elif isinstance(value, unicode):
			return value
	if isinstance(value, collections.Mapping):
		return dict(map(_ensure_unicode, value.items()))
	elif isinstance(value, collections.Iterable):
		return type(value)(map(_ensure_unicode, value))
	return value

# An internal method for generating a JWT authorization header based on
# the specified claim and key.
def _gen_auth_jwt_header(claim, key):
	if 'exp' not in claim:
		claim = copy.copy(claim)
		claim['exp'] = calendar.timegm(datetime.utcnow().utctimetuple()) + 3600
	else:
		claim = claim

	token = _ensure_unicode(jwt.encode(claim, key))

	return 'Bearer ' + token
