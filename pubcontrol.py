import time
import copy
import json
from base64 import b64encode
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

class PubControl(object):
	def __init__(self, uri):
		self.uri = uri
		self.thread = None
		self.sockid = id(self)
		self.sock = None
		self.auth_basic_user = None
		self.auth_basic_pass = None
		self.auth_jwt_claim = None
		self.auth_jwt_key = None

	def set_auth_basic(self, username, password):
		self.auth_basic_user = username
		self.auth_basic_pass = password

	def set_auth_jwt(self, claim, key):
		self.auth_jwt_claim = claim
		self.auth_jwt_key = key

	def publish(self, channel, item):
		i = item.export()
		i["channel"] = channel
		PubControl._pubcall(self.uri, self._gen_auth_header(), [i])

	# callback: func(boolean success, string message)
	# note: callback occurs in separate thread
	def publish_async(self, channel, item, callback=None):
		self._ensure_thread()
		i = item.export()
		i["channel"] = channel
		self.sock.send(pickle.dumps(("pub", self.uri, self._gen_auth_header(), i, callback)))

	def finish(self):
		if self.thread:
			self.sock.send(pickle.dumps(("stop",)))
			self.thread.join()
			self.thread = None
			self.sock.linger = 0
			self.sock.close()
			self.sock = None

	def _gen_auth_header(self):
		if self.auth_basic_user:
			return "Basic " + b64encode("%s:%s" % (self.auth_basic_user, self.auth_basic_pass))
		elif self.auth_jwt_claim:
			if "exp" not in self.auth_jwt_claim:
				claim = copy.copy(self.auth_jwt_claim)
				claim["exp"] = int(time.time()) + 600
			else:
				claim = self.auth_jwt_claim
			return "Bearer " + jwt.encode(claim, self.auth_jwt_key)
		else:
			return None

	def _ensure_thread(self):
		if self.thread is None:
			cond = threading.Condition()
			cond.acquire()
			self.thread = threading.Thread(target=PubControl._pubworker, args=(cond, self.sockid))
			self.thread.daemon = True
			self.thread.start()
			cond.wait()
			cond.release()
			self.sock = g_ctx.socket(zmq.PUB)
			self.sock.connect("inproc://publish-%d" % self.sockid)

	@staticmethod
	def _pubcall(uri, auth_header, items):
		uri = uri + "/publish/"

		headers = dict()
		if auth_header:
			headers["Authorization"] = auth_header
		headers["Content-Type"] = "application/json"

		content = dict()
		content["items"] = items
		content_raw = json.dumps(content)
		if isinstance(content_raw, unicode):
			content_raw = content_raw.encode("utf-8")

		try:
			urllib2.urlopen(urllib2.Request(uri, content_raw, headers))
		except Exception as e:
			raise ValueError("failed to publish: " + repr(e.read()))

	# reqs: list of (uri, auth_header, item, callback)
	@staticmethod
	def _pubbatch(reqs):
		assert(len(reqs) > 0)
		uri = reqs[0][0]
		auth_header = reqs[0][1]
		items = list()
		callbacks = list()
		for req in reqs:
			items.append(req[2])
			callbacks.append(req[3])

		try:
			PubControl._pubcall(uri, auth_header, items)
			result = (True, "")
		except Exception as e:
			result = (False, e.message)

		for c in callbacks:
			if c:
				c(result[0], result[1])

	@staticmethod
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
			reqs.append((m[1], m[2], m[3], m[4]))

			for n in range(0, 9):
				try:
					buf = sock.recv(zmq.NOBLOCK)
				except zmq.ZMQError as e:
					if e.errno == zmq.EAGAIN:
						break

				m = pickle.loads(buf)
				if m[0] == "stop":
					quit = True
					break

				reqs.append((m[1], m[2], m[3], m[4]))

			if len(reqs) > 0:
				PubControl._pubbatch(reqs)

		sock.linger = 0
		sock.close()
