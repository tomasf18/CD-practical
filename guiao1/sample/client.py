##!/usr/bin/env python

import socket

HOST = 'localhost'          # Aqui, quero o IP do server, não o meu!
PORT = 9010                 # Tem de ser o mesmo PORT a partir do qual o server está à escuta!

communication = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Como não estamos a hospedar (hosting), mas sim a tentar conectar com algo, apenas usamos "connect()"
# Precisamos de nos conectar à ligação onde o server se encontra ativo à espera de aceitar em "accept()"
communication.connect((HOST, PORT))

# Quero estar sempre a enviar mensagens!
while True:
    # Perguntar ao cliente qual mensagem quer enviar
    messageToSend = input("Mensagem que queres enviar: ")

    # Enviar a mensagem codificada
    communication.sendall(messageToSend.encode('utf-8'))

    # Receber agora o que o server tem para nos mandar!
    server_message = communication.recv(1024)
    print('Received form server:\n {}'.format(server_message.decode('utf-8')))

# Aqui, o socket "communication" conecta com o socket "cli_serv_communic", E NÃO COM "server"! 
# Ou seja, um endpoint da comunicação é "communication" e o outro é "cli_serv_communic". Entre estes 2 estabelece-se como que um túnel para as mensagems serem trocadas


# Mas como é que nós sabemos se devemos fazer um send() ou um recv()? E se o server envia algo antes de eu dizer que quero receber, porque eu ainda estou a 
#escrever o que vou enviar? E se acontecerem os 2 ao mesmo tempo? Solução: SELECTORS!
