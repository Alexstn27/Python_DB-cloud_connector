import paho.mqtt.client as mqtt
import mysql.connector
import sqlite3
import base64
import string
import mysql.connector
from mysql.connector.constants import ClientFlag


#Subscribing to the topics
def on_connect(client, userdata, flags, rc):
    print("Subscrubed to MQTT")
    client.subscribe("project-software-engineering/devices/pywierden/up")
    client.subscribe("project-software-engineering/devices/pygarage/up")
    client.subscribe("project-software-engineering/devices/pygronau/up")
    client.subscribe("project-software-engineering/devices/pysaxion/up")

#Gets the payload message from sensors, decode it,connect to the cloud and put the data there,
#retrieve in from cloud, print it on screen and write all the data on the file
def on_message(client, userdata, msg):
    config = {
    'user': 'root',
    'password': 'Banane@123',
    'host': '34.91.66.254',
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': 'server-ca.pem',
    'ssl_cert': 'client-cert.pem',
    'ssl_key': 'client-key.pem'
    }
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()  # initialize connection cursor
    config['database'] = 'Weatherdata'  # add new database to config dict
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    msgstr= str(msg.payload)
    location1=(msg.topic).find('devices')
    location2=(msg.topic).find('up')
    Location=str(msg.topic)[location1+8:location2-1]  
    pos=msgstr.find("payload_raw")+14
    msgraw=msgstr[pos:pos+8]
    payload=base64.decodebytes(msgraw.encode('utf-8'))
    Pressure=str(payload[0]/2+950)
    Light=str(payload[1])
    Temperature=str(((payload[2]-20)*10+payload[3])/10)
    
    query = ("INSERT INTO Data (Degrees,Pressure,Light,Location) "
         "VALUES (%s,%s,%s,%s)")
    val=(Temperature,Pressure,Light,Location)

    # then we execute with every row in our dataframe
    cursor.execute(query, val)
    print("Data was inserted in cloud")
    cnxn.commit()  # and commit changes
    f = open("myfiledata.txt", "w")
    f = open("myfiledata.txt", "a")
    cursor.execute("SELECT * FROM Data")
    print(val)
    out= cursor.fetchall()
    for row in out:
        print(row)
        line=str(row)
        f.write(line+'\n')
        
    f.close()
  
  
 
    
  
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set('project-software-engineering', 'ttn-account-v2.OC-mb7b1C5rDmos7-XTSoNE5T85V3c20jnrE8uN4jS0')
client.connect("eu.thethings.network", 1883, 60)

print(client.on_message)

client.loop_forever()
