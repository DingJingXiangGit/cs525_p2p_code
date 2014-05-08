from datastore import DataStore
from mode import PeerNode
from random import randint
from logistic_regression import D2LogisticRegression
from mlengine import MLEngine
from socket import *
import threading
import json
import zmq
import sys

Local_URL = "tcp://127.0.0.1:5556"
Local_URL_FORMAT = "tcp://{}:{}"
REMOTE_HANDLER_URL = "tcp://*:5557"
REMOTE_HANDLER_URL_FORMAT = "tcp://{}:{}"
TIME_BOUND = 1000
RETRY_TIMES = 5



class Task:
	def __init__(self, request, socket, addr):
		print(request)
		self.radiant = request["radiant"]
		self.dire = request["dire"]
		self.socket = socket
		self.addr = addr
		self.camp = request["camp"]
		self.counter = 2
		self.result = []
		self.mutex = threading.Lock()
		self.request = request

	def consume(self, entries):
		self.mutex.acquire()
		self.result.extend(entries)
		self.counter -= 1
		print("counter = "+str(self.counter))
		self.mutex.release()

	def finished(self):
		return self.counter == 0



class TaskHandler:
	def __init__(self):
		self.datastore = DataStore()
		self.peers = self.datastore.get_highest_rating(2)
		self.socket_table = {}
		self.poller = zmq.Poller()
		self.initialize_connections()


		self.task_sema = threading.Semaphore(0)
		self.task_mutex = threading.Lock()
		self.predictor = MLEngine(D2LogisticRegression())
		self.broadcast_thread = threading.Thread(target=self.start_process, args = ())
		self.broadcast_thread.start()
		


	def initialize_connections(self):
		context = zmq.Context()
		for peer in self.peers:
			socket_item = context.socket(zmq.REQ)
			print("connect to: tcp://{}:{}".format(peer.ip, peer.port))
			socket_item.connect ("tcp://{}:{}".format(peer.ip, peer.port))
			self.poller.register(socket_item, zmq.POLLIN)
			self.socket_table[peer.id] = socket_item

	def join(self):
		self.broadcast_thread.join()

	def launch_task(self, task):
		print("launch task.")
		self.current_task = task
		self.task_sema.release()

		local_result = self.predictor.recommend(task.radiant, task.dire)
		self.current_task.consume(local_result)
		self.check_and_done()

	def check_and_done(self):
		self.task_mutex.acquire()
		if self.current_task != None and self.current_task.finished():
			#recommendations = self.recommender.recommend(command["radiant"], command["dire"]);
			result = self.process_responses(self.current_task.result)
			json_str = json.dumps(result);
			self.current_task.socket.sendto(json_str.encode("utf-8"), self.current_task.addr)
			print("response message:"+json_str)
			self.current_task = None

		self.task_mutex.release()

	def process_responses(self,responses):
		result = {}
		result["candidates"] = []

		print("process_response: \t"+json.dumps(responses))
		for i in range(len(responses)):
			result["candidates"].append({"heroId": responses[i][1], "rate":responses[i][0]})
		return result

	def start_process(self):
		self.task_sema.acquire()
		print("process task.")
		for pid in self.socket_table:
			socket = self.socket_table[pid]
			message = json.dumps(self.current_task.request)
			print("send message to {} {}", pid, message)
			socket.send_string(message)

		result = []
		active_entry = []
		for count in range(RETRY_TIMES):
			socks = dict(self.poller.poll(TIME_BOUND))
			if socks:
				for socket_item in socks:
					if socks[socket_item] == zmq.POLLIN:
						print("sid = {}", socket_item)
						message = socket_item.recv()
						message = message.decode("utf-8")
						element = json.loads(message)
						active_entry.append({"ip":element["ip"], "port":element["port"]})
						print(message)
						result.extend(element["candidates"])

			if len(result) >= 5:
				break


		# delete dead peers[start]
		dead_list = []
		for peer  in self.peers:
			active  = False
			for entry in active_entry:
				if peer.ip == entry["ip"] and peer.port == entry["port"]:
					active = True
				if not active:
					dead_list.append(peer.id)

		for pid in dead_list:
			self.poller.unregister(self.socket_table[pid])
			self.socket_table[pid].close()
			self.socket_table.pop(pid, None)

		self.peers = [peer for peer in self.peers if peer.id not in dead_list]
		# delete dead peers[end]


		# return result
		print("peers size ", len(self.peers))
		print("remote resulting message {}".format(json.dumps(result)))
		self.current_task.consume(result)
		self.check_and_done()
		return result



class Engine:
	def __init__(self, local_ip, local_port, remote_ip, remote_port):
		self.recommender = MLEngine(D2LogisticRegression())
		self.local_ip = local_ip
		self.local_port = local_port
		self.remote_ip = remote_ip
		self.remote_port = remote_port

		self.task_handler = TaskHandler()
		self.client_thread = threading.Thread(target=self.start_task_creator, args = ())
		self.remote_thread = threading.Thread(target=self.start_remote_handler, args = ())
		self.client_thread.start()
		self.remote_thread.start()

		self.client_thread.join()
		self.remote_thread.join()
		self.task_handler.join()


	def start_task_creator(self):
		address = (self.local_ip, int(self.local_port))
		server_socket = socket(AF_INET, SOCK_DGRAM)
		server_socket.bind(address)
		while True:
			print("root receive new task [start]")
			content, addr = server_socket.recvfrom(2048)
			print("root:"+content.decode("utf-8"))
			command = json.loads(content.decode("utf-8"))
			responses = self.task_handler.launch_task(Task(command, server_socket, addr))
			print("root receive new task [end]")
			#server_socket.sendto("hello c#".encode("utf-8"), addr)

	def process_request(self, request):
		return self.recommender.recommend(request["radiant"], request["dire"])

	def start_remote_handler(self):
		url = REMOTE_HANDLER_URL_FORMAT.format(self.remote_ip, self.remote_port)
		context = zmq.Context()
		socket = context.socket(zmq.REP)
		socket.bind(url)
		result = {}
		while True:
			message = socket.recv()
			message = message.decode("utf-8")
			print("receive remote request: {}",message)
			request = json.loads(message)
			result["candidates"] = self.process_request(request)
			result["ip"] = self.remote_ip
			result["port"] = self.remote_port
			socket.send_string(json.dumps(result))
			print("send remote response: {}",json.dumps(result))




if __name__ == "__main__":
	if len (sys.argv) != 5:
		print("python engine.py <local ip> <local port> <remote handler ip> <remote handler port>")
	engine = Engine(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])