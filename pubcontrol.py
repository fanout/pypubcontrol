import time
import json
import urllib2
import threading
import pickle
import jwt
import zmq

g_ctx = zmq.Context()

def _make_token(realm, secret):
	claim = dict()
	claim["iss"] = realm
	claim["exp"] = int(time.time()) + 600
	return jwt.encode(claim, secret)

def _pubcall(uri, realm, secret, channel, items_json):
	uri = uri + "/publish/" + channel + "/"

	headers = dict()
	if realm:
		headers["Authorization"] = "Bearer %s" % _make_token(realm, secret)
	headers["Content-Type"] = "application/json"

	content = dict()
	content["items"] = items_json
	content_raw = json.dumps(content)
	if isinstance(content_raw, unicode):
		content_raw = content_raw.encode("utf-8")

	try:
		urllib2.urlopen(urllib2.Request(uri, content_raw, headers))
	except Exception as e:
		print "warning: failed to publish: " + e.message

def _pubbatch(reqs):
	assert(len(reqs) > 0)
	uri = reqs[0][0]
	realm = reqs[0][1]
	secret = reqs[0][2]
	channel = reqs[0][3]
	items_json = list()
	for req in reqs:
		items_json.append(req[4])
	_pubcall(uri, realm, secret, channel, items_json)

def _pubworker(cond, sockid):
	sock = g_ctx.socket(zmq.SUB)
	sock.bind("inproc://publish-%d" % sockid)
	cond.acquire()
	cond.notify()
	cond.release()

	while True:
		# block until a request is ready, then read many if possible
		buf = sock.recv()
		reqs = list()
		reqs.append(pickle.loads(buf))
		for n in range(0, 99):
			try:
				buf = sock.recv(zmq.NOBLOCK)
			except zmq.ZMQError as e:
				if e.errno == zmq.EAGAIN:
					break
			reqs.append(pickle.loads(buf))

		# batch reqs by same realm/channel
		batch = list()
		for req in reqs:
			if len(batch) > 0:
				last = batch[-1]
				if req[0] != last[0] or req[1] != last[1] or req[2] != last[2] or req[3] != last[3]:
					_pubbatch(batch)
					batch = list()

			batch.append(req)

		if len(batch) > 0:
			_pubbatch(batch)

# returns (boolean is_text, string value)
def bin_or_text(s):
	if isinstance(s, unicode):
		return (True, s)
	for c in s:
		i = ord(c)
		if i < 0x20 or i >= 0x7f:
			return (False, s)
	return (True, s.decode("utf-8"))

class Format(object):
	def name(self):
		pass

	def export(self):
		pass

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
			out["id"] = self.id
		if self.prev_id:
			out["prev-id"] = self.prev_id
		for f in self.formats:
			out[f.name()] = f.export()
		return out

class PubControl(object):
	def __init__(self):
		self.uri = None
		self.realm = None
		self.secret = None
		self.pubthread = None
		self.pubsockid = id()
		self.pubsock = None

	def _queue_publish(self, uri, realm, secret, channel, item):
		if self.pubthread is None:
			cond = threading.Condition()
			cond.acquire()
			self.pubthread = threading.Thread(target=_pubworker, args=(cond, self.pubsockid))
			self.pubthread.daemon = True
			self.pubthread.start()
			cond.wait()
			cond.release()
			self.pubsock = g_ctx.socket(zmq.PUB)
			self.pubsock.connect("inproc://publish-%d" % self.pubsockid)

		self.pubsock.send(pickle.dumps((uri, realm, secret, channel, item.export())))

	def publish(self, channel, item):
		assert(self.uri)
		self._queue_publish(self.uri, self.realm, self.secret, channel, item)
