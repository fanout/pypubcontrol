import time
import json
import urllib2
import threading
import pickle
import jwt
import zmq

g_ctx = zmq.Context()

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

def _make_token(realm, secret):
	claim = dict()
	claim["iss"] = realm
	claim["exp"] = int(time.time()) + 600
	return jwt.encode(claim, secret)

def _pubcall(uri, realm, secret, channel, items):
	uri = uri + "/publish/" + channel + "/"

	headers = dict()
	if realm:
		headers["Authorization"] = "Bearer %s" % _make_token(realm, secret)
	headers["Content-Type"] = "application/json"

	content = dict()
	content["items"] = items
	content_raw = json.dumps(content)
	if isinstance(content_raw, unicode):
		content_raw = content_raw.encode("utf-8")

	try:
		urllib2.urlopen(urllib2.Request(uri, content_raw, headers))
	except Exception as e:
		print "warning: failed to publish: " + repr(e.read())

def _pubbatch(reqs):
	assert(len(reqs) > 0)
	uri = reqs[0][1]
	realm = reqs[0][2]
	secret = reqs[0][3]
	channel = reqs[0][4]
	items = list()
	for req in reqs:
		items.append(req[5])
	_pubcall(uri, realm, secret, channel, items)

def _pubworker(cond, sockid):
	sock = g_ctx.socket(zmq.SUB)
	sock.setsockopt(zmq.SUBSCRIBE, "")
	sock.bind("inproc://publish-%d" % sockid)
	cond.acquire()
	cond.notify()
	cond.release()

	quit = False
	while not quit:
		# block until a request is ready, then read many if possible
		buf = sock.recv()
		m = pickle.loads(buf)

		if m[0] == "stop":
			break

		reqs = list()
		reqs.append(m)

		for n in range(0, 99):
			try:
				buf = sock.recv(zmq.NOBLOCK)
			except zmq.ZMQError as e:
				if e.errno == zmq.EAGAIN:
					break

			m = pickle.loads(buf)
			if m[0] == "stop":
				quit = True
				break

			reqs.append(m)

		# batch reqs by same realm/channel
		batch = list()
		for req in reqs:
			if len(batch) > 0:
				last = batch[-1]
				if req[1] != last[1] or req[2] != last[2] or req[3] != last[3] or req[4] != last[4]:
					_pubbatch(batch)
					batch = list()

			batch.append(req)

		if len(batch) > 0:
			_pubbatch(batch)

class PubControl(object):
	def __init__(self):
		self.uri = None
		self.realm = None
		self.secret = None
		self.thread = None
		self.sockid = id(self)
		self.sock = None

	def _queue_publish(self, uri, realm, secret, channel, item):
		if self.thread is None:
			cond = threading.Condition()
			cond.acquire()
			self.thread = threading.Thread(target=_pubworker, args=(cond, self.sockid))
			self.thread.daemon = True
			self.thread.start()
			cond.wait()
			cond.release()
			self.sock = g_ctx.socket(zmq.PUB)
			self.sock.connect("inproc://publish-%d" % self.sockid)

		self.sock.send(pickle.dumps(("item", uri, realm, secret, channel, item.export())))

	def publish(self, channel, item):
		assert(self.uri)
		self._queue_publish(self.uri, self.realm, self.secret, channel, item)

	def finish(self):
		self.sock.send(pickle.dumps(("stop",)))
		self.thread.join()
		self.thread = None
		self.sock.linger = 0
		self.sock.close()
		self.sock = None
