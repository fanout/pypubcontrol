import sys
import unittest
import copy
from base64 import b64encode, b64decode
import threading
import time
import socket
import json
import jwt

try:
	import urllib.request as urllib2
except ImportError:
	import urllib2

sys.path.append('../')
from src.pubcontrolclient import PubControlClient
from src.item import Item
from src.format import Format

class TestServer(object):
	def __init__(self):
		cond = threading.Condition()
		result = {}
		self.thread = threading.Thread(target=TestServer.server, args=(cond, result))
		self.thread.daemon = True
		cond.acquire()
		self.thread.start()
		cond.wait()
		self.port = result['port']
		cond.release()

	def wait_finish(self):
		self.thread.join()

	@staticmethod
	def server(cond, result):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.settimeout(1)
		s.bind(('', 0))
		s.listen(1)

		cond.acquire()
		result['port'] = s.getsockname()[1]
		cond.notify()
		cond.release()

		# close immediately
		conn, _ = s.accept()
		conn.close()

		# respond success
		conn, _ = s.accept()
		conn.recv(1024)
		conn.sendall('HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 3\r\n\r\nOk\n')
		conn.close()

		# respond with 500
		conn, _ = s.accept()
		conn.recv(1024)
		conn.sendall('HTTP/1.0 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: 6\r\n\r\nError\n')
		conn.close()

		# respond success
		conn, _ = s.accept()
		conn.recv(1024)
		conn.sendall('HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 3\r\n\r\nOk\n')
		conn.close()

		# respond with 500
		conn, _ = s.accept()
		conn.recv(1024)
		conn.sendall('HTTP/1.0 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: 6\r\n\r\nError\n')
		conn.close()

		# respond with 500
		conn, _ = s.accept()
		conn.recv(1024)
		conn.sendall('HTTP/1.0 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: 6\r\n\r\nError\n')
		conn.close()

class TestFormatSubClass(Format):
	def name(self):
		return 'name'

	def export(self):
		return {'body': 'bodyvalue'}

class PubControlClientForTesting(PubControlClient):
	def set_test_instance(self, instance):
		self.test_instance = instance

class PccForPublishTesting(PubControlClientForTesting):
	def _pubcall(self, uri, auth_header, items):
		self.test_instance.assertEqual(uri, 'uri')
		self.test_instance.assertEqual(auth_header, 'Basic ' + str(b64encode(
				'user:pass'.encode('ascii'))))
		self.test_instance.assertEqual(items, [{'name': {'body': 'bodyvalue'},
				'channel': 'chann'}])

	def _queue_req(self, req):
		self.test_instance.assertEqual(req[0], 'pub')
		self.test_instance.assertEqual(req[1], 'uri')
		self.test_instance.assertEqual(req[2], 'Basic ' + str(b64encode(
				'user:pass'.encode('ascii'))))
		self.test_instance.assertEqual(req[3], {'name': {'body': 'bodyvalue'},
				'channel': 'chann'})
		self.test_instance.assertEqual(req[4], 'callback')

	def _ensure_thread(self):
		self.ensure_thread_executed = True

class PccForPubCallTesting(PubControlClientForTesting):
	def set_params(self, uri, content_raw, headers, result_failure = False):
		self.http_uri = uri
		self.http_content_raw = content_raw
		self.http_headers = headers
		self.http_result_failure = result_failure

	def _make_http_request(self, uri, content_raw, headers):
		self.test_instance.assertEqual(uri, self.http_uri + '/publish/')
		self.test_instance.assertEqual(content_raw.decode('utf-8'), json.dumps(self.http_content_raw))
		self.test_instance.assertEqual(headers, self.http_headers)
		if self.http_result_failure:
			raise ValueError('test failure')

class PccForPubBatchTesting(PubControlClientForTesting):
	def set_params(self, result_failure, num_callbacks):
		self.req_index = 0
		self.num_callbacks = num_callbacks
		self.http_result_failure = result_failure

	def _pubcall(self, uri, auth_header, items):
		self.test_instance.assertEqual(uri, 'uri')
		self.test_instance.assertEqual(auth_header, 'Basic ' + str(b64encode(
				('user:pass' + str(self.req_index)).encode('ascii'))))
		items_to_compare_with = []
		export = Item(TestFormatSubClass()).export()
		export['channel'] = 'chann'
		for n in range(0, self.num_callbacks):
			items_to_compare_with.append(copy.deepcopy(export))
		self.req_index += 1
		self.test_instance.assertEqual(items, items_to_compare_with)
		if self.http_result_failure:
			raise ValueError('error message')

class PccForPubWorkerTesting(PubControlClientForTesting):
	def set_params(self):
		self.req_index = 0

	def _pubbatch(self, reqs):
		self.test_instance.assertTrue(len(reqs) <= 10, 'len(reqs) == ' +
				str(len(reqs)))
		for req in reqs:
			self.test_instance.assertEqual(req[0], 'uri')
			self.test_instance.assertEqual(req[1], 'Basic ' + str(b64encode(
					('user:pass' + str(self.req_index)).encode('ascii'))))
			export = Item(TestFormatSubClass()).export()
			export['channel'] = 'chann'
			self.test_instance.assertEqual(req[2], export)
			self.test_instance.assertEqual(req[3], 'callback')
			self.req_index += 1

class TestPubControlClient(unittest.TestCase):
	def test_initialize(self):
		pcc = PubControlClient('uri')
		self.assertEqual(pcc.uri, 'uri')
		self.assertEqual(pcc.thread, None)
		self.assertEqual(pcc.thread_cond, None)
		self.assertEqual(len(pcc.req_queue), 0)
		self.assertEqual(pcc.auth_basic_user, None)
		self.assertEqual(pcc.auth_basic_pass, None)
		self.assertEqual(pcc.auth_jwt_claim, None)
		self.assertEqual(pcc.auth_jwt_key, None)
		self.assertEqual(pcc.sub_monitor, None)
		self.assertTrue(pcc.lock != None)

	def test_set_auth_basic(self):
		pcc = PubControlClient('uri')
		pcc.set_auth_basic('user', 'pass')
		self.assertEqual(pcc.auth_basic_user, 'user')
		self.assertEqual(pcc.auth_basic_pass, 'pass')

	def test_set_auth_jwt(self):
		pcc = PubControlClient('uri')
		pcc.set_auth_jwt('claim', 'key')
		self.assertEqual(pcc.auth_jwt_claim, 'claim')
		self.assertEqual(pcc.auth_jwt_key, 'key')

	def test_ensure_thread(self):
		pcc = PubControlClient('uri')
		pcc._ensure_thread()
		try:
			self.assertTrue(isinstance(pcc.thread_cond, threading._Condition))
		except AttributeError:
			self.assertTrue(isinstance(pcc.thread_cond, threading.Condition))
		self.assertTrue(isinstance(pcc.thread, threading.Thread))

	def test_queue_req(self):
		pcc = PubControlClient('uri')
		pcc._ensure_thread()
		pcc._queue_req('req')
		self.assertEqual(pcc.req_queue.popleft(), 'req')

	def test_gen_auth_header_basic(self):
		pcc = PubControlClient('uri')
		pcc.set_auth_basic('user', 'pass')
		self.assertEqual(pcc._gen_auth_header(), 'Basic ' + str(b64encode(
				'user:pass'.encode('ascii'))))

	def test_gen_auth_header_jwt(self):
		pcc = PubControlClient('uri')
		pcc.set_auth_jwt({'iss': 'hello', 'exp': 1426106601},
				b64decode('key=='))
		self.assertEqual(pcc._gen_auth_header(), 'Bearer ' + jwt.encode(
				{'iss': 'hello', 'exp': 1426106601},
				b64decode('key==')).decode('utf-8'))

	def test_gen_auth_header_none(self):
		pcc = PubControlClient('uri')
		self.assertEqual(pcc._gen_auth_header(), None)

	def test_wait_all_sent(self):
		pcc = PubControlClient('uri')
		self.thread_for_testing_finish_completed = False
		pcc.thread_cond = threading.Condition()
		pcc.thread = threading.Thread(target = self.thread_for_testing_finish)
		pcc.thread.daemon = True
		pcc.thread.start()
		pcc.wait_all_sent()
		self.assertTrue(self.thread_for_testing_finish_completed)
		self.assertEqual(pcc.thread, None)
		self.assertEqual(pcc.req_queue.popleft(), ('stop',))

	def test_close(self):
		pcc = PubControlClient('uri')
		self.thread_for_testing_finish_completed = False
		pcc.thread_cond = threading.Condition()
		pcc.thread = threading.Thread(target = self.thread_for_testing_finish)
		pcc.thread.daemon = True
		pcc.thread.start()
		pcc.close()
		self.assertTrue(self.thread_for_testing_finish_completed)
		self.assertEqual(pcc.thread, None)
		self.assertEqual(pcc.req_queue.popleft(), ('stop',))

	def test_finish(self):
		pcc = PubControlClient('uri')
		self.thread_for_testing_finish_completed = False
		pcc.thread_cond = threading.Condition()
		pcc.thread = threading.Thread(target = self.thread_for_testing_finish)
		pcc.thread.daemon = True
		pcc.thread.start()
		pcc.finish()
		self.assertTrue(self.thread_for_testing_finish_completed)
		self.assertEqual(pcc.thread, None)
		self.assertEqual(pcc.req_queue.popleft(), ('stop',))

	def thread_for_testing_finish(self):
		time.sleep(1)
		self.thread_for_testing_finish_completed = True

	def test_publish(self):
		pcc = PccForPublishTesting('uri')
		pcc.set_auth_basic('user', 'pass')
		pcc.set_test_instance(self)
		pcc.publish('chann', Item(TestFormatSubClass()), False, 'callback')
		self.assertTrue(pcc.ensure_thread_executed)

	def test_publish_blocking(self):
		pcc = PccForPublishTesting('uri')
		pcc.set_auth_basic('user', 'pass')
		pcc.set_test_instance(self)
		pcc.publish('chann', Item(TestFormatSubClass()), True, 'callback')

	def test_pubcall_success_http(self):
		pcc = PccForPubCallTesting('uri')
		pcc.set_auth_basic('user', 'pass')
		pcc.set_params('http://localhost:8080', {'items':
				[{'name': {'body': 'bodyvalue'}, 'channel': 'chann'}]},
				{ 'Authorization': 'Basic ' + str(b64encode('user:pass'.encode('ascii'))),
				'Content-Type': 'application/json' })
		pcc.set_test_instance(self)
		pcc._pubcall('http://localhost:8080', 'Basic ' + str(
				b64encode('user:pass'.encode('ascii'))), [{'name': {'body': 'bodyvalue'},
				'channel': 'chann'}])

	def test_pubcall_success_https(self):
		pcc = PccForPubCallTesting('uri')
		pcc.set_auth_jwt({'iss': 'hello', 'exp': 1426106601},
				b64decode('key=='))
		pcc.set_params('https://localhost:8080', {'items':
				[{'name': {'body': 'bodyvalue'}, 'channel': 'chann'}]},
				{ 'Authorization': 'Bearer eyJhbGciOiJIU' +
				'zI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJoZWxsbyIsImV4cCI6MTQyNjEwN' +
				'jYwMX0.qmFVZ3iS041fAhqHno0vYLykNycT40ntBuD3G7ISDJw',
				'Content-Type': 'application/json' })
		pcc.set_test_instance(self)
		pcc._pubcall('https://localhost:8080', 'Bearer eyJhbGciOiJIU' +
				'zI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJoZWxsbyIsImV4cCI6MTQyNjEwN' +
				'jYwMX0.qmFVZ3iS041fAhqHno0vYLykNycT40ntBuD3G7ISDJw',
				[{'name': {'body': 'bodyvalue'},
				'channel': 'chann'}])

	def test_pubcall_failure(self):
		pcc = PccForPubCallTesting('uri')
		pcc.set_params('https://localhost:8080', {'items':
				[{'name': {'body': 'bodyvalue'}, 'channel': 'chann'}]},
				{ 'Content-Type': 'application/json' }, True)
		pcc.set_test_instance(self)
		try:
			pcc._pubcall('https://localhost:8080', None, [{'name':
					{'body': 'bodyvalue'}, 'channel': 'chann'}])
			self.assertTrue(False)
		except ValueError as e:
			self.assertTrue(str(e).index('test failure'))

	def pubbatch_callback(self, result, message):
		self.num_cbs_expected -= 1
		self.assertEqual(result, self.result_expected)
		self.assertEqual(message, self.message_expected)

	def test_pubbatch_success(self):
		pcc = PccForPubBatchTesting('uri')
		pcc.set_test_instance(self)
		self.result_expected = True
		self.message_expected = ''
		self.num_cbs_expected = 5
		pcc.set_params(None, self.num_cbs_expected)
		reqs = []
		export = Item(TestFormatSubClass()).export()
		export['channel'] = 'chann'
		for n in range(0, self.num_cbs_expected):
			reqs.append(['uri', 'Basic ' + str(b64encode(
					('user:pass' + str(n)).encode('ascii'))),
					export, self.pubbatch_callback])
		pcc._pubbatch(reqs)
		self.assertEqual(self.num_cbs_expected, 0)

	def test_pubbatch_failure(self):
		pcc = PccForPubBatchTesting('uri')
		pcc.set_test_instance(self)
		self.result_expected = False
		self.message_expected = 'error message'
		self.num_cbs_expected = 5
		pcc.set_params(True, self.num_cbs_expected)
		reqs = []
		export = Item(TestFormatSubClass()).export()
		export['channel'] = 'chann'
		for n in range(0, self.num_cbs_expected):
			reqs.append(['uri', 'Basic ' + str(b64encode(('user:pass' +
					str(n)).encode('ascii'))),
					export, self.pubbatch_callback])
		pcc._pubbatch(reqs)
		self.assertEqual(self.num_cbs_expected, 0)

	def test_pubworker(self):
		pcc = PccForPubWorkerTesting('uri')
		pcc.set_test_instance(self)
		pcc.set_params()
		pcc._ensure_thread()
		export = Item(TestFormatSubClass()).export()
		export['channel'] = 'chann'
		for n in range(0, 500):
			pcc.req_queue.append(('pub', 'uri',
					'Basic ' + str(b64encode(('user:pass' + str(n)).encode('ascii'))),
					export, 'callback'))
		pcc.finish()
		self.assertEqual(pcc.req_index, 500)

	def test_pubworker_stop(self):
		pcc = PccForPubWorkerTesting('uri')
		pcc.set_test_instance(self)
		pcc.set_params()
		pcc._ensure_thread()
		export = Item(TestFormatSubClass()).export()
		export['channel'] = 'chann'
		for n in range(0, 500):
			if n == 250:
				pcc.req_queue.append(['stop'])
			else:
				pcc.req_queue.append(['pub', 'uri',
						'Basic ' + str(b64encode(('user:pass' +
						str(n)).encode('ascii'))), export, 'callback'])
		pcc.finish()
		self.assertEqual(pcc.req_index, 250)

	def test_verify_status_code(self):
		pcc = PubControlClient('uri')
		pcc._verify_status_code(200, '')
		pcc._verify_status_code(250, '')
		pcc._verify_status_code(299, '')
		with self.assertRaises(ValueError):
			pcc._verify_status_code(199, '')
		with self.assertRaises(ValueError):
			pcc._verify_status_code(300, '')

	def test_retries(self):
		# this test assumes total retries is 1 (i.e. 2 attempts)

		server = TestServer()
		pcc = PubControlClient('http://127.0.0.1:{}'.format(server.port))

		# first request will fail, second will succeed
		pcc.publish('test', Item(TestFormatSubClass()), blocking=True)

		# first request will fail, second will succeed
		pcc.publish('test', Item(TestFormatSubClass()), blocking=True)

		# first and second requests will fail
		with self.assertRaises(ValueError):
			pcc.publish('test', Item(TestFormatSubClass()), blocking=True)

		server.wait_finish()

if __name__ == '__main__':
	unittest.main()
