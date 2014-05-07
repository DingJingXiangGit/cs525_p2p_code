class PeerNode:
	def __init__(self, id = -1, name = "", ip = "", port = -1, rating = 0):
		self.id = id
		self.name = name
		self.ip = ip
		self.port = port
		self.rating = rating

	def __str__(self):
		return "(id:{}, name:{}, ip:{}, port:{}, rating:{})".format(self.id, self.name, self.ip, self.port, self.rating)