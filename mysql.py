import MySQLdb


class AtemMySQL:
	conn = ""
	x = ""
	
	def __init__(self, nodeName):
		self.nodeName = nodeName
		
	def init(self):
		self.conn = MySQLdb.connect(host= "localhost",
						  user="airtem",
						  passwd="legolego",
						  db="airtem")
		self.x = self.conn.cursor()
	
	def close(self):
		self.conn.close()
	
	def insertBme(self, temp, humidity, pressure):
		cmd = "INSERT INTO %s (temperature, humidity, pressure) VALUES ('%s','%s','%s') ON DUPLICATE KEY UPDATE temperature = '%s', humidity = '%s', pressure = '%s'" % (self.nodeName, temp, humidity, pressure, temp, humidity, pressure)
		try:
			self.x.execute(cmd)
			self.conn.commit()
			return 0
		except exception as e:
			print e
			self.conn.rollback()
		print "Failed to create entry: %s" % (cmd)
		return -1
	
	def insertCO2(self, co2):
		cmd = "INSERT INTO %s (co2) VALUES ('%s') ON DUPLICATE KEY UPDATE co2 = '%s'" % (self.nodeName, co2, co2)
		try:
			self.x.execute(cmd)
			self.conn.commit()
			return 0
		except Exception as e:
			print e
			self.conn.rollback()
		print "Failed to create entry: %s" % (cmd)
		return -1
	
	def insertSts(self, message):
		cmd = "INSERT INTO %s_sts (message) VALUES ('%s')" % (self.nodeName, message)
		try:
			self.x.execute(cmd)
			self.conn.commit()
			return 0
		except Exception as e:
			print e
			self.conn.rollback()
		print "Failed to create entry: %s" % (cmd)
		return -1
	
	def insertEvent(self, event):
		cmd = "INSERT INTO %s (comment) VALUES ('%s') ON DUPLICATE KEY UPDATE comment = '%s'" % (self.nodeName, event, event)
		try:
			self.x.execute(cmd)
			self.conn.commit()
			return 0
		except Exception as e:
			print e
			self.conn.rollback()
		print "Failed to create entry: %s" % (cmd)
		return -1
	