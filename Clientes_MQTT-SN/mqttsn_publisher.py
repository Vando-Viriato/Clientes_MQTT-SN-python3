from MQTTSNclient import Client
import struct
import time
import sys
import xlrd


class Callback:

    def published(self, MsgId): #Função publish, publica o tópico e imprime na tela.
        print("Topico publicado!")

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
        print('Saindo...')
        sys.exit()

#Função para registro dos tópicos
#Registra o tópico e informa que o certo tópico foi registrado
def register_topic():
    global sensor1_battery_d15_d45_d75, sensor2_battery_d15_d45_d75, sensor3_battery_d15_d45_d75, sensor4_battery_d15_d45_d75, sensor5_battery_wetness_rain_temperature
    sensor1_battery_d15_d45_d75 = aclient.register("sensor1_battery_d15_d45_d75")
    print("Topico - sensor1_battery_d15_d45_d75 - foi registrado com sucesso.")
    print(" ")
    sensor2_battery_d15_d45_d75 = aclient.register("sensor2_battery_d15_d45_d75")
    print("Topico - sensor2_battery_d15_d45_d75 - foi registrado com sucesso.")
    print(" ")
    sensor3_battery_d15_d45_d75 = aclient.register("sensor3_battery_d15_d45_d75")
    print("Topico - sensor3_battery_d15_d45_d75 - foi registrado com sucesso.")
    print(" ")
    sensor4_battery_d15_d45_d75 = aclient.register("sensor4_battery_d15_d45_d75")
    print("Topico - sensor4_battery_d15_d45_d75 - foi registrado com sucesso.")
    print(" ")
    sensor5_battery_wetness_rain_temperature = aclient.register("sensor5_battery_wetness_rain_temperature")
    print("Topico - sensor5_battery_wetness_rain_temperature - foi registrado com sucesso.")
    print(" ")

#Cliente se concta ao gateway
aclient = Client("Client_SN_Pub", "localhost", port=10000)
aclient.registerCallback(Callback())
connect_gateway()

#Declarando os tópicos e chamando a função registrar
sensor1_battery_d15_d45_d75 = "sensor1_battery_d15_d45_d75"
sensor2_battery_d15_d45_d75 = "sensor2_battery_d15_d45_d75"
sensor3_battery_d15_d45_d75 = "sensor3_battery_d15_d45_d75"
sensor4_battery_d15_d45_d75 = "sensor4_battery_d15_d45_d75"
sensor5_battery_wetness_rain_temperature = "sensor5_battery_wetness_rain_temperature"
register_topic()

#TUDO SOBRE O PAYLOAD (CONTEUDO A SER PUBLICADO)

arq = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_1.xlsx") #Acessa o arquivo do sensor 1

wb = xlrd.open_workbook(arq) #Abre o arquivo atravez da função open da biblioteca xlrd
pag = wb.sheet_by_index(0)

pag.cell_value(0,0)
dados = pag.row_values(1, 0) #Atribue todas a colunas da linha 1 a variável dados.
salva = []
payload1 = 0
for i in dados:
    salva.append(str(i)) #Transforma conteudo de dados em uma string e adiciona a lista vazia (salva)
    payload1 = '---'.join(salva) #Tira os dados da lista salva e coloca na variável payload1
    #Cliente publish publica o payload1 no tópico (sensor1_battery_d15_d45_d75, payload1) com qos 0
pub_msgid = aclient.publish(sensor1_battery_d15_d45_d75, payload1, qos=0)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS DIFERENTES

arq2 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_2.xlsx")

wb2 = xlrd.open_workbook(arq2)
pag2 = wb2.sheet_by_index(0)

pag2.cell_value(0,0)
dados2 = pag2.row_values(1, 0)
salva2 = []
payload2 = 0
for i2 in dados2:
    salva2.append(str(i2))
    payload2 = '---'.join(salva2)
pub_msgid = aclient.publish(sensor2_battery_d15_d45_d75, payload2, qos=1)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS DIFERENTES

arq3 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_3.xlsx")

wb3 = xlrd.open_workbook(arq3)
pag3 = wb3.sheet_by_index(0)

pag3.cell_value(0,0)
dados3 = pag3.row_values(1, 0)
salva3 = []
payload3 = 0
for i3 in dados3:
    salva3.append(str(i3))
    payload3 = '---'.join(salva3)
pub_msgid = aclient.publish(sensor3_battery_d15_d45_d75, payload3, qos=1)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS DIFERENTES

arq4 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_4.xlsx")

wb4 = xlrd.open_workbook(arq4)
pag4 = wb4.sheet_by_index(0)

pag4.cell_value(0,0)
dados4 = pag4.row_values(1, 0)
salva4 = []
payload4 = 0
for i4 in dados4:
    salva4.append(str(i4))
    payload4 = '---'.join(salva4)
pub_msgid = aclient.publish(sensor4_battery_d15_d45_d75, payload4, qos=2)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS DIFERENTES

arq5 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_5.xlsx")

wb5 = xlrd.open_workbook(arq5)
pag5 = wb5.sheet_by_index(0)

pag5.cell_value(0,0)
dados5 = pag5.row_values(1, 0)
salva5 = []
payload5 = 0
for i5 in dados5:
    salva5.append(str(i5))
    payload5 = '---'.join(salva5)
pub_msgid = aclient.publish(sensor5_battery_wetness_rain_temperature, payload5, qos=2)
time.sleep(40) #Dorme 40 segundos e faz outra publicação

#LENDO E PUBLICANDO SEGUNDA LINHA DE DADOS DOS SENSORS
#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS E NÚMERO DE LINHA DIFERENTES

arq = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_1.xlsx")

wb = xlrd.open_workbook(arq)
pag = wb.sheet_by_index(0)

pag.cell_value(0,0)
dados = pag.row_values(2, 0)
salva = []
payload1 = 0
for i in dados:
    salva.append(str(i))
    payload1 = '---'.join(salva)
pub_msgid = aclient.publish(sensor1_battery_d15_d45_d75, payload1, qos=0)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS E NÚMERO DE LINHA DIFERENTES

arq2 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_2.xlsx")

wb2 = xlrd.open_workbook(arq2)
pag2 = wb2.sheet_by_index(0)

pag2.cell_value(0,0)
dados2 = pag2.row_values(2, 0)
salva2 = []
payload2 = 0
for i2 in dados2:
    salva2.append(str(i2))
    payload2 = '---'.join(salva2)
pub_msgid = aclient.publish(sensor2_battery_d15_d45_d75, payload2, qos=1)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS E NÚMERO DE LINHA DIFERENTES

arq3 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_3.xlsx")

wb3 = xlrd.open_workbook(arq3)
pag3 = wb3.sheet_by_index(0)

pag3.cell_value(0,0)
dados3 = pag3.row_values(2, 0)
salva3 = []
payload3 = 0
for i3 in dados3:
    salva3.append(str(i3))
    payload3 = '---'.join(salva3)
pub_msgid = aclient.publish(sensor3_battery_d15_d45_d75, payload3, qos=1)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS E NÚMERO DE LINHA DIFERENTES

arq4 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_4.xlsx")

wb4 = xlrd.open_workbook(arq4)
pag4 = wb4.sheet_by_index(0)

pag4.cell_value(0,0)
dados4 = pag4.row_values(2, 0)
salva4 = []
payload4 = 0
for i4 in dados4:
    salva4.append(str(i4))
    payload4 = '---'.join(salva4)
pub_msgid = aclient.publish(sensor4_battery_d15_d45_d75, payload4, qos=2)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS E NÚMERO DE LINHA DIFERENTES

arq5 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_5.xlsx")

wb5 = xlrd.open_workbook(arq5)
pag5 = wb5.sheet_by_index(0)

pag5.cell_value(0,0)
dados5 = pag5.row_values(2, 0)
salva5 = []
payload5 = 0
for i5 in dados5:
    salva5.append(str(i5))
    payload5 = '---'.join(salva5)
pub_msgid = aclient.publish(sensor5_battery_wetness_rain_temperature, payload5, qos=2)
time.sleep(40) #Dorme 40 segundos e faz outra publicação

#LENDO E PUBLICANDO TERCEIRA LINHA DE DADOS DOS SENSORS
#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS E NÚMERO DE LINHA DIFERENTES

arq = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_1.xlsx")

wb = xlrd.open_workbook(arq)
pag = wb.sheet_by_index(0)

pag.cell_value(0,0)
dados = pag.row_values(3, 0)
salva = []
payload1 = 0
for i in dados:
    salva.append(str(i))
    payload1 = '---'.join(salva)
pub_msgid = aclient.publish(sensor1_battery_d15_d45_d75, payload1, qos=0)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS E NÚMERO DE LINHA DIFERENTES

arq2 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_2.xlsx")

wb2 = xlrd.open_workbook(arq2)
pag2 = wb2.sheet_by_index(0)

pag2.cell_value(0,0)
dados2 = pag2.row_values(3, 0)
salva2 = []
payload2 = 0
for i2 in dados2:
    salva2.append(str(i2))
    payload2 = '---'.join(salva2)
pub_msgid = aclient.publish(sensor2_battery_d15_d45_d75, payload2, qos=1)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS E NÚMERO DE LINHA DIFERENTES

arq3 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_3.xlsx")

wb3 = xlrd.open_workbook(arq3)
pag3 = wb3.sheet_by_index(0)

pag3.cell_value(0,0)
dados3 = pag3.row_values(3, 0)
salva3 = []
payload3 = 0
for i3 in dados3:
    salva3.append(str(i3))
    payload3 = '---'.join(salva3)
pub_msgid = aclient.publish(sensor3_battery_d15_d45_d75, payload3, qos=1)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS E NÚMERO DE LINHA DIFERENTES

arq4 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_4.xlsx")

wb4 = xlrd.open_workbook(arq4)
pag4 = wb4.sheet_by_index(0)

pag4.cell_value(0,0)
dados4 = pag4.row_values(3, 0)
salva4 = []
payload4 = 0
for i4 in dados4:
    salva4.append(str(i4))
    payload4 = '---'.join(salva4)
pub_msgid = aclient.publish(sensor4_battery_d15_d45_d75, payload4, qos=2)

#TODOS OS COMENTÁRIOS DA LINHA 62 A 76 SE APLICAM AO TRECHO A SEGUIR, COM VARIÁVEIS E NÚMERO DE LINHA DIFERENTES

arq5 = ("/home/alcindo/Área de Trabalho/TCC/Dados/modulo_5.xlsx")

wb5 = xlrd.open_workbook(arq5)
pag5 = wb5.sheet_by_index(0)

pag5.cell_value(0,0)
dados5 = pag5.row_values(3, 0)
salva5 = []
payload5 = 0
for i5 in dados5:
    salva5.append(str(i5))
    payload5 = '---'.join(salva5)
pub_msgid = aclient.publish(sensor5_battery_wetness_rain_temperature, payload5, qos=2)


try:
    while True:
        time.sleep(1) #Dorme um segundo volta
except KeyboardInterrupt:
    aclient.disconnect() #Se houver interrupção via teclado o cliente se desconecta
    print("Client_SN_Pub desconectado do Gateway.")
