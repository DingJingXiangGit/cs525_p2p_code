import sqlite3 as lite
from mode import PeerNode
import sys
import threading
con = None
CREATE_TABLE_CMD = "CREATE TABLE if not exists Peers(Id INTEGER PRIMARY KEY, Name TEXT, IP Text, Port Text, Rating Int)"
SELECT_ALL_CMD = "SELECT * FROM Peers;"
INSERT_CMD = "INSERT INTO Peers (Name, IP, Port, Rating) VALUES(?, ?, ?, ?);"
UPDATE_CMD = "UPDATE Peers SET Name=?,Rating=? where IP=? and Port=?;"
DELETE_ALL_CMD = "DELETE FROM Peers;"
DELETE_CMD = "DELETE FROM Peers WHERE Id = {};"
GET_HIGHEST_RATING = "SELECT * FROM Peers ORDER BY Rating DESC LIMIT {};"



class DataStore:
	mutex = threading.Lock()
	def __init__(self):
		self.con = lite.connect('./cs525.db')
		with self.con:
   			cursor = self.con.cursor()
   			cursor.execute(CREATE_TABLE_CMD)


	def get_all_peers(self):
		peer_nodes = []
		with self.con:
			cursor = self.con.cursor()
			cursor.execute(SELECT_ALL_CMD)
			rows = cursor.fetchall()
			for row in rows:
				peer_nodes.append(PeerNode(row[0], row[1], row[2], row[3],row[4]))

		return peer_nodes

	def insert_peers(self, node_list):
		print (self.con)
		with self.con:
			for node in node_list:
				cursor = self.con.cursor()
				cursor.execute(INSERT_CMD, [node.name, node.ip, node.port, node.rating])
				node.id = cursor.lastrowid

	def update(self, node):
		with self.con:
			cursor = self.con.cursor()
			cursor.execute(UPDATE_CMD, [node.name, node.rating, node.ip, node.port])

	def delete(self, node):
		with self.con:
			cursor = self.con.cursor()
			print(DELETE_CMD.format(node.id))
			cursor.execute(DELETE_CMD.format(node.id))

	def clear_all_peers(self):
		with self.con:
			for node in node_list:
				cursor = self.con.cursor()
				cursor.execute(DELETE_ALL_CMD)

	def get_highest_rating(self, num):
		peer_nodes = []
		with self.con:
			cursor = self.con.cursor()
			query = GET_HIGHEST_RATING.format(num)
			cursor.execute(query)
			rows = cursor.fetchall()
			for row in rows:
				peer_nodes.append(PeerNode(row[0], row[1], row[2], row[3],row[4]))

		return peer_nodes

	def close(self):
		self.con.close()


if __name__ == "__main__":
    datastore = DataStore()
    node_list = []
    node_list.append(PeerNode(0, "node-1", "127.0.0.1", 8001, 21))
    node_list.append(PeerNode(0, "node-2", "127.0.0.1", 8002, 22))
    node_list.append(PeerNode(0, "node-3", "127.0.0.1", 8003, 23))
    node_list.append(PeerNode(0, "node-4", "127.0.0.1", 8004, 24))
    node_list.append(PeerNode(0, "node-5", "127.0.0.1", 8005, 15))
    node_list.append(PeerNode(0, "node-6", "127.0.0.1", 8006, 16))
    node_list.append(PeerNode(0, "node-7", "127.0.0.1", 8007, 17))
    node_list.append(PeerNode(0, "node-8", "127.0.0.1", 8008, 18))
    node_list.append(PeerNode(0, "node-9", "127.0.0.1", 8009, 19))
    node_list.append(PeerNode(0, "node-10", "127.0.0.1", 8010, 20))
    datastore.insert_peers(node_list)
    nodes = datastore.get_highest_rating(5)
    print("print initial nodes:")
    for node in nodes:
    	print(node)
    #datastore.clear_all_peers()