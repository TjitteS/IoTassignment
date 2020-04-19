from flask import Flask, render_template, request
import getpass

app = Flask(__name__)

import sqlite3

p = getpass.getpass() 
# Retrieve data from database
def getData():
    
    import pyodbc
    server='tjits.database.windows.net'
    database='tjitsbase'
    uid='tjitte'
    pwd=p
    driver= '{FreeTDS}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+uid+';PWD='+ pwd)

    cursor = cnxn.cursor()
    for row in cursor.execute("SELECT TOP 20 pc.Name as CategoryName, p.name as ProductName FROM [SalesLT].[ProductCategory] pc JOIN [SalesLT].[Product] p ON pc.productcategoryid = p.productcategoryid"):
        time = str(row[0])
        temp = row[1]
        hum = row[2]
    conn.close()
    return time, temp, hum

# main route 
@app.route("/")
def index():	
	time, temp, hum = getData()
	templateData = {
		'time': time,
		'temp': temp,
		'hum': hum
	}
	return render_template('index.html', **templateData)

if __name__ == "__main__":
   app.run(host='0.0.0.0', port=80, debug=True)