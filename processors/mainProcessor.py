import json
import sys
import os, urlparse
import paho.mqtt.client as mqtt
import pymysql
import datetime
import threading
import requests

class Individuo:
    def __init__(self, idRecinto, mac, mediaPot, qtdOcorrencias, momentoChegada):
        self.__idRecinto      = idRecinto
        self.__mac            = mac
        self.__mediaPot       = mediaPot
        self.__qtdOcorrencias = qtdOcorrencias
        self.__momentoChegada = momentoChegada
        self.__contador       = 60
        self.__momentoSaida   = None

    def getIdRecinto(self):
        return self.__idRecinto

    def setIdRecinto(self, idRecinto):
        self.__idRecinto = idRecinto

    def getMac(self):
        return self.__mac

    def setMac(self, mac):
        self.__mac = mac

    def getMediaPot(self):
        return self.__mediaPot

    def setMediaPot(self, mediaPot):
        self.__mediaPot = mediaPot

    def getQtdOcorrencias(self):
        return self.__qtdOcorrencias

    def setQtdOcorrencias(self, qtdOcorrencias):
        self.__qtdOcorrencias = qtdOcorrencias

    def getMomentoChegada(self):
        return self.__momentoChegada

    def setMomentoChegada(self, momentoChegada):
        self.__momentoChegada = momentoChegada

    def getMomentoSaida(self):
        return self.__momentoSaida

    def setMomentoSaida(self, momentoSaida):
        self.__momentoSaida = momentoSaida

    def getContador(self):
        return self.__contador

    def setContador(self, contador):
        self.__contador = contador

    def decrementaContador(self):
        self.__contador -= 1

    def getTempoPermanencia(self):
        return datetimeString2Sec(self.__momentoSaida) - datetimeString2Sec(self.__momentoChegada)

tempDic = {}

def datetimeString2Sec(string):
    timestamp = datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
    timestamp = datetime.timedelta(hours=timestamp.hour, minutes=timestamp.minute, seconds=timestamp.second)
    return timestamp.total_seconds()

def validaAparicao(mediaPot, qtdOcorrencias):
    if((float(mediaPot) >= -80.0) and (qtdOcorrencias >= 3)):
        return True
    else:
        return False

def insereNaLista(individuo, momentoAtual):
    if((individuo.getMac(), individuo.getIdRecinto()) not in tempDic):
        tempDic[(individuo.getMac(), individuo.getIdRecinto())] = individuo
    else:
        tempDic[(individuo.getMac(), individuo.getIdRecinto())].setContador(60)
        tempDic[(individuo.getMac(), individuo.getIdRecinto())].setMomentoSaida(momentoAtual)

def removeIndividuos():
    tempListDeletados = []
    tempListEnviados = []

    for (macIndividuo, idRecintoIndividuo), individuo in tempDic.items():
        individuo.decrementaContador()

    for (macIndividuo, idRecintoIndividuo) in tempDic:
        if(tempDic[(macIndividuo, idRecintoIndividuo)].getContador() == 0):
            tempListDeletados.append((macIndividuo, idRecintoIndividuo))
            if(tempDic[(macIndividuo, idRecintoIndividuo)].getMomentoSaida() != None):
                toBeSent = json.dumps({'idRecinto': int(tempDic[(macIndividuo, idRecintoIndividuo)].getIdRecinto()),\
                            'mac': macIndividuo, \
                            'momentoChegada': tempDic[(macIndividuo, idRecintoIndividuo)].getMomentoChegada(),\
                            'momentoSaida': tempDic[(macIndividuo, idRecintoIndividuo)].getMomentoSaida(),\
                            'tempoPermanencia': tempDic[(macIndividuo, idRecintoIndividuo)].getTempoPermanencia() })
                tempListEnviados.append(toBeSent)

    for (macIndividuo, idRecintoIndividuo) in tempListDeletados:
        del tempDic[(macIndividuo, idRecintoIndividuo)]

    print(tempListEnviados)
    for obj in tempListEnviados:
        requests.post('http://6c65b423.ngrok.io80/individuo', data=obj, auth=('admin', 'admin'))

    threading.Timer(1.0, removeIndividuos).start()

threading.Timer(1.0, removeIndividuos).start()


def on_connect_filaDadosSpark(self, mosq, obj, rc):
    print("rc_filaDadosSpark: " + str(rc))
 
def on_message_filaDados_Spark(mosq, obj, msg):

    mensagem = str(msg.payload)
    mensagem = mensagem.replace("(", "")
    mensagem = mensagem.replace(")", "")
    mensagem = mensagem.replace("u'", "")
    mensagem = mensagem.replace("'", "")
    mensagem = mensagem.replace(" ", "")

    lista = mensagem.split(",")

    idRecinto      = lista[0]
    mac            = lista[1]
    mediaPot       = lista[2]
    qtdOcorrencias = lista[3]
    momentoAtual   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    individuo = Individuo(idRecinto, mac, mediaPot, qtdOcorrencias, momentoAtual)

    if(validaAparicao(mediaPot, qtdOcorrencias)):
        insereNaLista(individuo, momentoAtual)
        print("\n<---InicioDaLista--->")
        for (macIndividuo, idRecintoIndividuo) in tempDic:
            print("MaC>(" + macIndividuo + ") ConTaDoR>(" + str(tempDic[(macIndividuo, idRecintoIndividuo)].getContador()) + \
             ") CheGaDa>(" + tempDic[(macIndividuo, idRecintoIndividuo)].getMomentoChegada() + ") sAiDa>(" + str(tempDic[(macIndividuo, idRecintoIndividuo)].getMomentoSaida()) + ")")
        print("<---FimDaLista--->")

 
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