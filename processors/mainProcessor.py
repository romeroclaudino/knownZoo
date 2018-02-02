import json
import sys
import os, urlparse
import paho.mqtt.client as mqtt
import pymysql
from datetime import datetime
 
conn = pymysql.connect(
    db='base_zoo',
    user='root',
    passwd='root',
    host='localhost')
 
cursorBD = conn.cursor()
queryInsert  = "INSERT INTO dados_spark (recinto, mac, pot, qtd, momento_captura) VALUES ('%s', '%s', '%s', '%s', now())"
 
def insereMac(recinto, mac,pot,qtd):

    query = queryInsert % (recinto, mac, pot, qtd)
    print(query)
    cursorBD.execute(query)
    conn.commit()
 
    return 

def on_connect_filaDadosSpark(self, mosq, obj, rc):
    print("rc_filaDadosSpark: " + str(rc))
 
def on_message_filaDados_Spark(mosq, obj, msg):
    mensagem = str(msg.payload)
    mensagem = mensagem.replace("(", "")
    mensagem = mensagem.replace(")", "")
    mensagem = mensagem.replace("u'", "")
    mensagem = mensagem.replace("'", "")

    lista = mensagem.split(",")
    print(lista)

    recinto = lista[0]
    mac =  lista[1]
    pot =  lista[2]
    qtd =  lista[3]

    insereMac(recinto, mac, pot, qtd)

 
def on_subscribe_filaDadosSpark(mosq, obj, mid, granted_qos):
    print("Subscribed_filaDadosSpark: " + str(mid) + " " + str(granted_qos))

def on_log(mosq, obj, level, string):
    print("On_Log: " + string)
 
mqttFilaDadosSpark = mqtt.Client()
 
mqttFilaDadosSpark.on_message = on_message_filaDados_Spark
mqttFilaDadosSpark.on_connect = on_connect_filaDadosSpark
mqttFilaDadosSpark.on_subscribe = on_subscribe_filaDadosSpark
 
 
url_str = os.environ.get('localhost','tcp://localhost:1883')
url = urlparse.urlparse(url_str)
 
mqttFilaDadosSpark.username_pw_set("psd", "psd")
mqttFilaDadosSpark.connect(url.hostname, url.port)
 
 
mqttFilaDadosSpark.subscribe("dadosSpark", 0)
 
rcFilaDadosSpark = 0
 
 
while rcFilaDadosSpark == 0:
    rcFilaDadosSpark = mqttFilaDadosSpark.loop()