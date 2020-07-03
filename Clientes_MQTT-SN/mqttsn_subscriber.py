
from MQTTSNclient import Client
import struct
import time
import sys


class Callback:
#      Função de mensagem recebida (quando chegar alguma mensagem imprime o conteudo e o tópico)
    def messageArrived(self, topicName, payload, qos, retained, msgid):
        print(" ")
        print('Recebendo a mensagem %s do %s' % (payload, topicName))
        return True

#Função que conecta o cliente ao gateway
def connect_gateway():
    try:
        while True:
            try:
                aclient.connect() #Se a conexão for bem sucedida, imprime conectado ao gateway.
                print('Conectado ao gateway...')
                print(" ")
                break
            except: #Se não, informa a falha.
                print('Falha ao conectar ao gateway, reconectando!...')
                time.sleep(5) #Dorme 5 segundos e sai.
    except KeyboardInterrupt:
        print('Saindo!...')
        sys.exit()

#Função para se inscrever a um certo tópico (cliente se inscreve e na tela o nome do tópico inscrito)
def subscribe_topic():
    aclient.subscribe("sensor1_battery_d15_d45_d75", qos=0)
    print("Inscrito ao topico - sensor1_battery_d15_d45_d75")
    print(" ")
    aclient.subscribe("sensor2_battery_d15_d45_d75", qos=1)
    print("Inscrito ao topico - sensor2_battery_d15_d45_d75")
    print(" ")
    aclient.subscribe("sensor3_battery_d15_d45_d75", qos=1)
    print("Inscrito ao topico - sensor3_battery_d15_d45_d75")
    print(" ")
    aclient.subscribe("sensor4_battery_d15_d45_d75", qos=2)
    print("Inscrito ao topico - sensor4_battery_d15_d45_d75")
    print(" ")
    aclient.subscribe("sensor5_battery_wetness_rain_temperature", qos=2)
    print("Inscrito ao topico - sensor5_battery_wetness_rain_temperature")
    print(" ")

#Cliente se conecta ao gateway
aclient = Client("Client_SN_Sub", "localhost", port=10000)
aclient.registerCallback(Callback())
connect_gateway()

subscribe_topic()

try:
    while True:
        time.sleep(1) #Dorme um segundo volta
except KeyboardInterrupt:
    #SE HOUVER INTERRUPÇÃO VIA TECLADO CANCELA A INSCRIÇÃO DE TODOS OS TÓPICOS E DESCONECTA DO GATEWAY

    #aclient.unsubscribe('sensor1_battery_d15_d45_d75')
    #print("Cancelada a inscrição do topico - sensor1_battery_d15_d45_d75")
    #aclient.unsubscribe('sensor2_battery_d15_d45_d75')
    #print("Cancelada a inscrição do topico - sensor2_battery_d15_d45_d75")
    #aclient.unsubscribe('sensor3_battery_d15_d45_d75')
    #print("Cancelada a inscrição do topico - sensor3_battery_d15_d45_d75")
    #aclient.unsubscribe('sensor4_battery_d15_d45_d75')
    #print("Cancelada a inscrição do topico - sensor4_battery_d15_d45_d75")
    #aclient.unsubscribe('sensor5_battery_wetness_rain_temperature')
    #print("Cancelada a inscrição do topico - sensor5_battery_wetness_rain_temperature")
    aclient.disconnect()
    print("Client_SN_Sub desconectado do gateway.")


