from datastore import DataStore
from mode import PeerNode
from random import randint
from logistic_regression import D2LogisticRegression
from mlengine import MLEngine
#from socket import *
import socket
import time
import threading
import json, zmq, sys
import pickle, http.client, urllib.parse

Local_URL = "tcp://127.0.0.1:5556"
Local_URL_FORMAT = "tcp://{}:{}"
REMOTE_HANDLER_URL = "tcp://*:5557"
REMOTE_HANDLER_URL_FORMAT = "tcp://{}:{}"
TIME_BOUND = 1
RETRY_TIMES = 2
DBGMODE = True

class ModelLoader:
	def __init__(self, baseURL):
		self.url = baseURL

	def load(self):
		conn = http.client.HTTPConnection(self.url)
		conn.request("GET", "/loadmoel")
		res = conn.getresponse()
		status = False
		if res.status == 200:
			binary_model = res.read()
			model = pickle.loads(binary_model)
			status = True
		conn.close()
		return {"status": status, "model": model}

class HeartBeat:
	def __init__(self, baseURL, remote_ip, remote_port):
		self.url = baseURL
		self.period = 2;
		self.thread = threading.Timer(self.period, self.report)
		self.thread.daemon = True
		self.remote_ip = remote_ip
		self.remote_port = remote_port

	def report(self):
		peers = []
		try:
			params = urllib.parse.urlencode({'ip': self.remote_ip, 'port': self.remote_port})
			headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
			conn = http.client.HTTPConnection(self.url)
			conn.request("POST", "/heartbeat", params, headers)
			res = conn.getresponse()
			content = res.read()

			content = json.loads(content.decode("utf-8"))
			peers = content["peers"]
			peers = [node for node in peers if node["ip"] != self.remote_ip or node["port"] != self.remote_port]
			conn.close()
		except:
			pass
		
		self.update_database(peers)
		self.thread = threading.Timer(self.period, self.report)
		self.thread.start()

	def update_database(self, peers):
		DataStore.mutex.acquire()
		database_conn = DataStore()
		current_nodes = database_conn.get_all_peers()
		new_list = []
		for peer in peers:
			overlay = False
			for node in current_nodes:
				if node.ip == peer["ip"] and node.port == peer["port"]:
					if node.rating < 30:
						node.rating = 30
					database_conn.update(node)
					overlay = True

			if overlay == False:
				new_list.append(PeerNode(0, peer["id"], peer["ip"], peer["port"], 30))
		database_conn.insert_peers(new_list)
		database_conn.close()
		DataStore.mutex.release()


	def start(self):
		self.thread.start()

	def stop(self):
		self.thread.cancel()


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
		print("[start] consume = "+str(self.counter))
		self.mutex.acquire()
		self.result.extend(entries)
		self.counter -= 1
		self.mutex.release()
		print("[end] consume = "+str(self.counter))

	def finished(self):
		return self.counter == 0



class TaskHandler:
	PEER_REPICK = 5
	def __init__(self, remote_ip, remote_port):
		self.udp_sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.socket_table = {}
		#self.poller = zmq.Poller()
		#self.initialize_connections()
		self.remote_ip = remote_ip
		self.remote_port = remote_port
		self.task_sema = threading.Semaphore(0)
		self.task_mutex = threading.Lock()
		self.predictor = MLEngine(D2LogisticRegression())
		self.broadcast_thread = threading.Thread(target=self.start_process, args = ())
		self.broadcast_thread.start()
		
	def initialize_connections(self):
		if len(self.socket_table) < TaskHandler.PEER_REPICK:
			if DBGMODE : print("initialize_connections [start]")
			DataStore.mutex.acquire()
			self.datastore = DataStore()
			self.peers = self.datastore.get_highest_rating(10)
			for peer in self.peers:
				if peer.id not in self.socket_table and (peer.ip != self.remote_ip or peer.port !=self.remote_port):
					self.socket_table[peer.id] = (peer.ip, int(peer.port))
			self.datastore.close()
			DataStore.mutex.release()

	# def initialize_connections(self):
	# 	if len(self.socket_table) < TaskHandler.PEER_REPICK:
	# 		if DBGMODE : print("initialize_connections [start]")
	# 		DataStore.mutex.acquire()
	# 		self.datastore = DataStore()
	# 		self.peers = self.datastore.get_highest_rating(2)
	# 		context = zmq.Context()

	# 		for peer in self.peers:
	# 			if peer.id not in self.socket_table:
	# 				#print("connect to: tcp://{}".format(peer))
	# 				socket_item = context.socket(zmq.REQ)
	# 				socket_item.connect ("tcp://{}:{}".format(peer.ip, peer.port))
	# 				self.poller.register(socket_item, zmq.POLLIN)
	# 				self.socket_table[peer.id] = socket_item
	# 		self.datastore.close()
	# 		DataStore.mutex.release()
	# 		print("initialize_connections [end]")

	def join(self):
		self.broadcast_thread.join()

	def launch_task(self, task):
		if DBGMODE : print("launch_task [start]")
		self.current_task = task
		self.task_sema.release()
		if task.camp == 0:
			local_result = self.predictor.recommend(task.radiant, task.dire)
		else:
			local_result = self.predictor.recommend(task.dire, task.radiant)

		self.current_task.consume(local_result)
		self.check_and_done()
		if DBGMODE : print("launch_task [end]")

	def check_and_done(self):
		if DBGMODE : print("check_and_done [start]")
		self.task_mutex.acquire()
		if self.current_task != None and self.current_task.finished():
			#recommendations = self.recommender.recommend(command["radiant"], command["dire"]);
			result = self.process_responses(self.current_task.result)
			json_str = json.dumps(result);
			self.current_task.socket.sendto(json_str.encode("utf-8"), self.current_task.addr)
			print("response message:"+json_str)
			self.current_task = None

		self.task_mutex.release()
		if DBGMODE : print("check_and_done [end]")

	def process_responses(self,responses):
		if DBGMODE : print("process_responses [start]")
		result = {}
		result["candidates"] = []
		print("process_response: \t"+json.dumps(responses))
		for i in range(len(responses)):
			result["candidates"].append({"heroId": responses[i][1], "rate":responses[i][0]})
		if DBGMODE : print("process_responses [end]")
		return result

	def start_process(self):
		while True:
			self.initialize_connections()
			if DBGMODE : print("pre start process task.")
			self.task_sema.acquire()
			if DBGMODE : print("real process task.")
			for pid in self.socket_table:
				target = self.socket_table[pid]
				message = json.dumps(self.current_task.request)
				print(target)
				self.udp_sender.sendto(message.encode("utf-8"), target)

			result = []
			active_entry = []
			remain_time = RETRY_TIMES*TIME_BOUND
			start_time = time.time()
			count = 0;
			print("remain tiem is:"+str(remain_time))
			#for count in range(RETRY_TIMES):
			while True:
				try:
					print("[start] recvfrom remote")
					self.udp_sender.settimeout(remain_time)
					data,addr = self.udp_sender.recvfrom(4096)
					message = data.decode("utf-8")
					print("[middle] recvfrom:"+message)
					element = json.loads(message)
					active_entry.append({"ip":element["ip"], "port":element["port"]})
					result.extend(element["candidates"])
					count += 1;
					remain_time = remain_time + start_time - time.time()
					#print("[end] recvfrom remote: counter")
					#print("len = "+str(len(self.socket_table)))
					#print("count = "+str(count))
					#print("remain_time = "+str(remain_time))

					if len(self.socket_table) <= count or remain_time <= 0:
						break
				except:
					break

			dead_list = []
			for peer  in self.peers:
				active  = False
				for entry in active_entry:
					if peer.ip == entry["ip"] and peer.port == entry["port"]:
						active = True
					if not active:
						dead_list.append(peer.id)

			for pid in dead_list:
				#self.poller.unregister(self.socket_table[pid])
				print("close pid: "+str(pid))
				#self.socket_table[pid].close()
				self.socket_table.pop(pid, None)
			
			self.peers = [peer for peer in self.peers if peer.id not in dead_list]

			# delete dead peers[end]
			print("peers size ", len(self.peers))
			print("remote resulting message {}".format(json.dumps(result)))
			self.current_task.consume(result)
			self.check_and_done()

	# def start_process(self):
	# 	while True:
	# 		self.initialize_connections()
	# 		if DBGMODE : print("pre start process task.")
	# 		self.task_sema.acquire()
	# 		if DBGMODE : print("real process task.")
	# 		for pid in self.socket_table:
	# 			socket = self.socket_table[pid]
	# 			message = json.dumps(self.current_task.request)
	# 			print("send message to {} {}".format(pid, message))
	# 			socket.send_string(message)

	# 		result = []
	# 		active_entry = []
	# 		for count in range(RETRY_TIMES):
	# 			socks = dict(self.poller.poll(TIME_BOUND))
	# 			if socks:
	# 				for socket_item in socks:
	# 					if socks[socket_item] == zmq.POLLIN:
	# 						print("sid = {}", socket_item)
	# 						message = socket_item.recv()
	# 						message = message.decode("utf-8")
	# 						element = json.loads(message)
	# 						active_entry.append({"ip":element["ip"], "port":element["port"]})
	# 						print(message)
	# 						result.extend(element["candidates"])

	# 			if len(result) >= 5:
	# 				break


	# 		# delete dead peers[start]
	# 		dead_list = []
	# 		for peer  in self.peers:
	# 			active  = False
	# 			for entry in active_entry:
	# 				if peer.ip == entry["ip"] and peer.port == entry["port"]:
	# 					active = True
	# 				if not active:
	# 					dead_list.append(peer.id)

	# 		for pid in dead_list:
	# 			self.poller.unregister(self.socket_table[pid])
	# 			print("close pid: "+pid)
	# 			self.socket_table[pid].close()
	# 			self.socket_table.pop(pid, None)

	# 		self.peers = [peer for peer in self.peers if peer.id not in dead_list]
	# 		# delete dead peers[end]
	# 		print("peers size ", len(self.peers))
	# 		print("remote resulting message {}".format(json.dumps(result)))
	# 		self.current_task.consume(result)
	# 		self.check_and_done()



class Engine:
	def __init__(self, local_ip, local_port, remote_ip, remote_port):
		self.recommender = MLEngine(D2LogisticRegression())
		self.local_ip = local_ip
		self.local_port = local_port
		self.remote_ip = remote_ip
		self.remote_port = remote_port

		self.heart_beater = HeartBeat("54.186.108.36:8888", remote_ip, remote_port)
		self.task_handler = TaskHandler(remote_ip, remote_port)
		self.client_thread = threading.Thread(target=self.start_task_creator, args = ())
		self.remote_thread = threading.Thread(target=self.start_remote_handler, args = ())
		self.heart_beater.start()
		self.client_thread.start()
		self.remote_thread.start()

		self.client_thread.join()
		self.remote_thread.join()
		self.task_handler.join()
		self.heart_beater.stop()


	def start_task_creator(self):
		address = (self.local_ip, int(self.local_port))
		server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		server_socket.bind(address)
		while True:
			print("root receive new task [start]")
			content, addr = server_socket.recvfrom(2048)
			
			command = json.loads(content.decode("utf-8"))
			radiant = [i for i in command["radiant"] if i != -1 and i!=77]
			dire = [i for i in command["dire"] if i != -1 and i!=77]
			command["dire"] = dire
			command["radiant"] = radiant

			responses = self.task_handler.launch_task(Task(command, server_socket, addr))
			print("root receive new task [end]")
			#server_socket.sendto("hello c#".encode("utf-8"), addr)

	def process_request(self, request):
		if request["camp"] == 0:
			return self.recommender.recommend(request["radiant"], request["dire"])
		else:
			return self.recommender.recommend(request["dire"], request["radiant"])

	def start_remote_handler(self):
		sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM) # UDP
		sock.bind((self.remote_ip, int(self.remote_port)))
		result = {}
		while True:
			data, addr = sock.recvfrom(4096)
			message = data.decode("utf-8")
			print("receive remote request: {}",message)
			request = json.loads(message)
			result["candidates"] = self.process_request(request)
			result["ip"] = self.remote_ip
			result["port"] = self.remote_port
			sock.sendto(json.dumps(result).encode("utf-8"), addr)
			print("send remote response: {}",json.dumps(result))
	#def start_remote_handler(self):
	#	url = REMOTE_HANDLER_URL_FORMAT.format(self.remote_ip, self.remote_port)
	#	context = zmq.Context()
	#	socket = context.socket(zmq.REP)
	#	socket.bind(url)
	#	result = {}
	#	while True:
	#		message = socket.recv()
	#		message = message.decode("utf-8")
	#		print("receive remote request: {}",message)
	#		request = json.loads(message)
	#		result["candidates"] = self.process_request(request)
	#		result["ip"] = self.remote_ip
	#		result["port"] = self.remote_port
	#		socket.send_string(json.dumps(result))
	#		print("send remote response: {}",json.dumps(result))




if __name__ == "__main__":
	if len (sys.argv) != 5:
		print("python engine.py <local ip> <local port> <remote handler ip> <remote handler port>")
	engine = Engine(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])