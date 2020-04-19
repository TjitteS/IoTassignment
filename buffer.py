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

RESPONSE_MSGIDS = [];
RESPONSE_RESULT = [];

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    client.subscribe("devices/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global databuffer_db
    try:
        print(msg.payload)
        q="INSERT INTO buffer VALUES (\'%s\',\'%s\',\'new\'); "% (msg.topic, msg.payload.decode("utf-8"))
        print(q)
        c = databuffer_db.cursor();
        c.execute(q)
        databuffer_db.commit()
    except lite.Error as e:
       print("Error while inserting message in buffer: %s" % e)

async def sendBufferToHub():
    global databuffer_db, MESSAGE_COUNT, iotclient
    c = databuffer_db.cursor()
            
    #Sends all data from the buffer to the iot hub.
    
     #check if there is data buffered in the database
    while True:
        #fetch a row.
        row = None

        c.execute("SELECT ROWID, * FROM buffer WHERE state = 'new' ORDER BY ROWID ASC LIMIT 1")
        row = c.fetchone()
        
        data_db = lite.connect('data.db');
        datac = data_db.cursor();
        
        if(row != None):
            #there is data in the buffer!
            #try to send the row forward to IoT hub:
            timestamp = datetime.datetime.now().strftime("%B %d, %Y, %H.%M.%S")
                
            #format the message to be sent to the IoT hub: TODO
            MSG_TXT = "{\"deviceId\": \"%s\",%s}"
            msg_txt_formatted = MSG_TXT % (row[1],row[2])
            print (msg_txt_formatted)
            
            try:
                message =  Message(msg_txt_formatted, message_id="%s" % row[0])
                await asyncio.wait_for(iotclient.send_message(message), 2.0)
                
                #Format: "temperature": "%f","humidity\": "%f","edgetimestamp":"%s"
                split = [re.sub('[^0-9.]','',x) for x in row[2].split(',')]
                temp=split[0]
                hum=split[1]
                timestamp=split[2]
                q = "INSERT INTO rasp VALUES (%s, %s, %s, %s);" % (row[1], temp, hum, timestamp)
                print(q)
                datac.execute(q);
                
                #Send correctly! delete the row
                #update state:
                q = "DELETE FROM buffer WHERE ROWID = %s" % (row[0])
                print(q)
                c.execute(q)
            except asyncio.TimeoutError as e:
                #An Error accoured!
                print("An error acoured while trying to send iot message:")
                print(e);
                data_db.commit();
                data_db.close();
                
                #halt all tries to send current buffer
                return
            
        else:
            #there is no more data in the buffer.
            return
        
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
    global databuffer_db
    
    
    #infinite loop:
    while True:
        await sendBufferToHub()
        
        mqttclient.loop(0.1);
  
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
       data_db.close()
       
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
    
    asyncio.run(mainloop())
    close()
    
