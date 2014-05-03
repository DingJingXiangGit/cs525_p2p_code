import zmq
import time

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5556")
counter = 0
while True:
    message = socket.recv()
    counter += 1
    socket.send_string("word {} {}".format(counter, message))
    time.sleep(2)
