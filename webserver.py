from flask import Flask, render_template, request, send_file,Response
import getpass
import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import random
import sqlite3 as lite
import time
from datetime import datetime

app = Flask(__name__)

import sqlite3


 
    
def getData(n=1):
    cnxn = lite.connect('data.db')
    cursor = cnxn.cursor()
    
    time = [None]*n
    temp = [None]*n
    hum  = [None]*n

    i = 0
    for row in cursor.execute("SELECT * FROM  rasp ORDER BY timestamp DESC LIMIT %d"%n):
        time[i] = datetime.strptime(row[3], "%Y-%m-%d, %H.%M.%S.%f")
        temp[i] = float(row[1])
        hum[i]  = float(row[2])
        i += 1
    cnxn.commit()    
    cnxn.close()
    return time, temp, hum

def grapgen():
    while True:
        fig = Figure(figsize = (10, 5))
        axis1 = fig.add_subplot(1, 2, 1)
        axis2 = fig.add_subplot(1, 2, 2)
        
        time, temp, hum = getData(5000)
        axis1.plot(time, temp)
        axis2.plot(time, hum)
        
        output = io.BytesIO()
        FigureCanvas(fig).print_png(output, dpi=600)
        
        frame = output.getvalue()
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n')


@app.route("/graph")
def graph():
    return render_template('graph.html')
    
@app.route('/graphlive')
def render_gaph():
    return Response(grapgen(), mimetype='multipart/x-mixed-replace;  boundary=frame')

# main route 
@app.route("/")
def index():	
	time, temp, hum = getData(1)
	templateData = {
		'time': time[0],
		'temp': round(temp[0],2),
		'hum': round(hum[0],2)
	}
	return render_template('index.html', **templateData)
    


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True)