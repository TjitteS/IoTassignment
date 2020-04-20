#!/usr/bin/env python
import os
import sqlite3 as lite
import random
import time
import datetime
import sys
import config as config
import paho.mqtt.client as mqtt
import atexit
import uuid
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
import asyncio
from azure.iot.device import exceptions as iotex
import re


#Globals
mqttclient   = mqtt.Client()
databuffer_db = None
data_db = None
iotclient = None

msglist = []

def saveinbuffer(msg):
    global databuffer_db
    try:
        #print(msg.payload)
        q="INSERT INTO buffer VALUES (\'%s\',\'%s\',\'new\'); "% (msg.topic, msg.payload.decode("utf-8"))
        print("[%s] Message arrived in buffer with device ID: %s" % (time.strftime("%H:%M:%S", time.gmtime()),msg.topic))
        c = databuffer_db.cursor();
        c.execute(q)
        #databuffer_db.commit()
    except lite.Error as e:
       print("Error while inserting message in buffer: %s" % e)
       
       
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    client.subscribe("devices/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    msglist.append(msg)


# Retrieve data from database
def exstract(param, s):
    #Format: " ... "param":"lookingforthis?" ...
    #Find parameter keyword index
    i = s.find(param)
    #find separator index:
    i = s.find(':', i)

    #find string start and end index:
    istart = s.find('\"', i) +1
    iend = s.find('\"', istart)
    return s[istart:iend]

async def trysend(row):
    try:
        MSG_TXT = "{\"deviceId\": \"%s\",%s}"
        msg_txt_formatted = MSG_TXT % (row[1],row[2])
        message =  Message(msg_txt_formatted, message_id="%s" % row[0])
        await asyncio.wait_for(iotclient.send_message(message), 2.0)
        return True
    except asyncio.TimeoutError as e:
        #An Error accoured!
        return False
    

async def sendBufferToHub():
    global databuffer_db,data_db, MESSAGE_COUNT, iotclient
    c = databuffer_db.cursor()
    datac = data_db.cursor()
    
    c.execute("SELECT ROWID, * FROM buffer WHERE state = 'new' ORDER BY ROWID ASC LIMIT 1000")
    
    rows = c.fetchall()

    tasks = []
    for row in rows:
        tasks.append(trysend(row))
    #send
    res = await asyncio.gather(*tasks);
    
    for n in range(len(rows)):  
        if res[n] == True:
            row = rows[n];
            temp = exstract("temperature", row[2])
            hum  = exstract("humidity", row[2])
            timestamp=exstract("edgetimestamp", row[2])
                
            datac.execute("INSERT INTO [rasp] VALUES (\'%s\', \'%s\', \'%s\', \'%s\');" % (row[1], temp, hum, timestamp));
            c.execute(    "DELETE FROM buffer WHERE ROWID = %s" % (row[0]))
                
            print("[%s] Message sent to IoT Hub from device ID: %s"%(time.strftime("%H:%M:%S", time.gmtime()),row[1]) )
                
        else:
            print("a message was not send")
            
    data_db.commit();

    return
   
def close():
    global databuffer_db
    
    print("Saving database...")
    databuffer_db.commit();
    databuffer_db.close();
    print("stopping mqtt client...")
    mqttclient.loop_stop();
    print("Exiting...")
    
async def mainloop():
    global databuffer_db, msglist
    
    
    #infinite loop:
    while True:
        #mqttclient.loop(0.001);
  
        i=0
        while i < len(msglist):
            saveinbuffer(msglist[i])
            i += 1
        msglist = []
        
        await sendBufferToHub()
        
        databuffer_db.commit();
        
    return


if __name__ == "__main__":
    print ( "\nPython %s" % sys.version )
    print ( "IoT Hub Client for Python" )
    
    #register exit callback
    atexit.register(close)
    
    #connect to database:
    try:
       databuffer_db = lite.connect('buffer.db')
       sql_create_buffer_table = """ CREATE TABLE IF NOT EXISTS buffer (
                                        deviceid varchar(64),
                                        msg text,
                                        state text
                                    ); """
       c = databuffer_db.cursor();
       c.execute(sql_create_buffer_table) 
       
       data_db = lite.connect('data.db');
       sql_create_buffer_table = """ CREATE TABLE IF NOT EXISTS rasp (
                                        deviceid varchar(64),
                                        temperature text,
                                        humidity text,
                                        timestamp text
                                    ); """
       datac = data_db.cursor();
       datac.execute(sql_create_buffer_table)
       data_db.commit()
       #data_db.close()
       
       print("Connected to buffer database.")
    except lite.Error as e:
       print("Error initializing buffer database: %s" % e)
       sys.exit()
       
    #Connect to local MQTT broker
    mqttclient.on_connect = on_connect
    mqttclient.on_message = on_message
    mqttclient.connect(config.BROKER_IP, 1883, 60)

    #connect to IoT hub:
    iotclient =  IoTHubDeviceClient.create_from_connection_string(config.CONECITONSTRING)
    iotclient.connect()
    
    mqttclient.loop_start();
    asyncio.run(mainloop())
    close()
    
