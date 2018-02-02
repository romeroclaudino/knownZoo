import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from mqtt import MQTTUtils
import pika

credentials = pika.PlainCredentials('psd', 'psd')
connection = pika.BlockingConnection(pika.ConnectionParameters(
               '10.0.2.15', 5672, 'psd', credentials))
channel = connection.channel()
channel.exchange_declare(exchange='amq.topic',
                 exchange_type='topic', durable=True)

def publish(content):
    channel.basic_publish(
        exchange='amq.topic',  # amq.topic as exchange
        routing_key='dadosSpark',   # Routing key used by producer
        body=str(content)
    )


def extrai(rdd):
    lista = rdd.take(20)
    print(lista)
    if(len(lista) > 0):
        for item in lista:
            publish(item)
        

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: mqtt_wordcount.py <broker url> <topic>"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingMQTTWordCount")
    ssc = StreamingContext(sc, 10)

    brokerUrl = sys.argv[1]
    topic = sys.argv[2]

    lines = MQTTUtils.createStream(ssc, brokerUrl, topic)
    counts = lines.map(lambda line: line.split(" "))\
    .map(lambda vec: ((vec[0],vec[1]), (int(vec[2]), 1)))\
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
    .map(lambda tupla: (tupla[0], float(tupla[1][0])/tupla[1][1], tupla[1][1]))
    
    
    counts.pprint()
    counts.foreachRDD(extrai)

    ssc.start()
ssc.awaitTermination()
connection.close()
