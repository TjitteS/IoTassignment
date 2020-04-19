# IoTassignment
this repo contians an assignment for the corse IoT. 
buffer.py asusmes a mosqitio mqtt server is running on the local host. It subscrbes to all topics and puts the contend of the messageds in a database, buffer.db. The enetries in the database are then send to the IoT hub, and  deleted if the data is sucsesfully transfered.
