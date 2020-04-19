
import smbus2
import bme280
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import time
import datetime

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    #client.subscribe("devices/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))


port = 1
address = 0x76
bus = smbus2.SMBus(port)


calibration_params = bme280.load_calibration_params(bus, address)


while True:
    data = bme280.sample(bus, address, calibration_params)

    # there is a handy string representation too
    timestamp = datetime.datetime.now().strftime("%B %d, %Y, %H.%M.%S")
    payload = "\"temperature\": \"%f\",\"humidity\": \"%f\",\"edgetimestamp\":\"%s\"" % (data.temperature, data.humidity, timestamp)
    print(payload)
    publish.single("devices/local", payload, hostname="localhost")
        
    time.sleep(2);

