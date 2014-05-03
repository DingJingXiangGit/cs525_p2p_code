import zmq
import sys
import time
import json


if __name__ == "__main__":
	ip = sys.argv[1]
	port = sys.argv[2]

	target_url = "tcp://{}:{}".format(ip, port)
	context = zmq.Context()
	socket = context.socket(zmq.PAIR)
	socket.connect(target_url)
	print("1. remote_helper start")
	for i in range (10):
		request = {}
		request["content"] = {"radian":[10, 20, 30, 40, 50], "dire":[1, 2,]}
		socket.send_string(json.dumps(request))
		print("send message #{}: {}".format(i, json.dumps(request)))
		msg = socket.recv_string()
		print("receive message #{}: {}", i, msg)
		time.sleep(2)