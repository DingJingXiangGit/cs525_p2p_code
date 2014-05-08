import tornado.ioloop
import tornado.web
import pickle, os, random, time, json

storage = {}
NUM_OF_PEERS = 40

model_path = os.path.join('logistic_regression', 'model.pkl')
with open(model_path, 'rb') as input_file:
	learning_model = pickle.load(input_file)

binary_model = pickle.dumps(learning_model)


class ClientInfo:
	def __init__(self, ip, port, timestamp):
		self.ip = ip
		self.port = port
		self.timestamp = timestamp
		self.id = "{}:{}".format(ip, port)

	def __lt__(self, other):
		return self.timestamp < other.timestamp

class HeartBeatHandler(tornado.web.RequestHandler):
	def post(self):
		client_ip = self.get_argument('ip')
		client_port = self.get_argument('port')
		timestamp = time.time()
		identifier = "{}:{}".format(client_ip, client_port)
		if identifier in storage:
			client = storage[identifier]
			client.timestamp = timestamp
		else:
			client = ClientInfo(client_ip, client_port, timestamp)
			storage[client.id] = client

		clients = list(storage.values())
		sample = random.sample(clients, min(NUM_OF_PEERS, len(clients)))
		result = {"peers":[]}
		for client in sample:
			result["peers"].append({"ip":client.ip, "port":client.port, "id":client.id})
		self.write(json.dumps(result))


class ModelHandler(tornado.web.RequestHandler):
	def get(self):
		self.write(binary_model)

class DefaultHandler(tornado.web.RequestHandler):
	def get(self):
		self.write("hello world")


application = tornado.web.Application([
    (r"/heartbeat", HeartBeatHandler),
    (r"/loadmodel", ModelHandler),
    (r"/", DefaultHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()