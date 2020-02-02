#!/usr/bin/env python
import paho.mqtt.client as mqtt #import the client
import time
import argparse

from mysql import AtemMySQL
from credentials import *


flag_connected = 0

def on_connect(client, userdata, flags, rc):
   global flag_connected
   flag_connected = 1

def on_disconnect(client, userdata, rc):
   global flag_connected
   flag_connected = 0

def insertData(data):
	if data[0] == "BME":
		mysql.insertBme(data[1], data[2], data[3])
	elif data[0] == "CO2":
		mysql.insertCO2(data[1])
	elif data[0] == "EV":
		mysql.insertEvent(data[1])
	elif data[0] == "STS":
		mysql.insertSts(data[1])
	else:
		print "Unsupported key %s!" % (data[0])

def on_message(client, userdata, message):
	msg = str(message.payload.decode("utf-8"))
	#print("message received ", msg)
	#print("message topic=",message.topic)
	#print("message qos=",message.qos)
	#print("message retain flag=",message.retain)

	mysql.init()
	msg = msg.split('\r\n')[0].split(',');
	insertData(msg)
	mysql.close

if __name__ == "__main__":
	#process passed argument
	usage = "usage: %prog nodeName"
	parser = argparse.ArgumentParser()
	parser.add_argument("nodeName", type=str,
						help="provide your node name")
	args = parser.parse_args()
	
	TOPICS = [("/atem/%s/sensors" % (args.nodeName), 0), ("/atem/%s/sts" % (args.nodeName), 1)]
	mysql = AtemMySQL(args.nodeName)
	
	global flag_connected
	while 1:
		print("creating new instance")
		client = mqtt.Client("P1") #create new instance
		client.	username_pw_set(MQTT_USER, password=MQTT_PASS)
		client.on_message=on_message #attach function to callback
		client.on_connect = on_connect
		client.on_disconnect = on_disconnect
		print("connecting to broker")
		client.connect(MQTT_HOST) #connect to broker
		client.loop_start() #start the loop
		print("Subscribing to topic", TOPICS)
		client.subscribe(TOPICS)
			
		while 1:
			try:
				client.loop()
			except Exception as e:
				print "Exception occured: %s" % (e)
				break
			if flag_connected == 0:
				break
		print "MQTT stop"
		client.loop_stop() #stop the loop