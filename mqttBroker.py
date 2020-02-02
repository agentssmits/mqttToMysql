import subprocess
import shlex
from credentials import *

class Broker:
	def __init__(self, nodeName, tableName):
		self.nodeName = nodeName
		self.tableName = tableName
		self.launch()
		
	def launch(self):
		cmd = "mosquitto_sub -h %s -t /atem/%s/%s -u \"%s\" -P \"%s\"" % (MQTT_REMOTE_HOST, self.nodeName, self.tableName, MQTT_USER, MQTT_PASS)
		try:
			self.proc = subprocess.Popen(shlex.split(cmd))
		except Exception as e:
			print "Broker launch for %s/%s failed" % (self.nodeName, self.tableName)
			print e
		
	def isRunning(self):
		if self.proc.poll() == None:
			return True
		return False
		
	def relaunch(self):
		if self.isRunning() == False:
			self.launch()
			
	def stop(self):
		self.proc.kill()
