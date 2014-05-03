import zmq
from random import randint
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.setsockopt(zmq.LINGER, 0)
socket.connect("tcp://localhost:5556")
print("connected")
#  Do 10 requests, waiting each time for a response
for request in range(1,10):
    socket.send_string("Hello {}".format(randint(0,100)))
    message = socket.recv()
    print ("Received reply {} {}".format( request, message,))
