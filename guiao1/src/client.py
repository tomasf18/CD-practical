"""CD Chat client program"""
import logging
import sys
import fcntl
import os
import socket
import selectors

from .protocol import CDProto, CDProtoBadFormat

logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)


class Client:
    """ Chat Client process """


    def __init__(self, name: str = "Foo"):
        """ Initializes chat client """

        self.name = name

        HOST = 'localhost'                                                      # IP do Server
        PORT = 9010                                                             # Porta onde o Server está à escuta
        self.client_address = (HOST, PORT)                                      # Criar endereço do cliente

        self.cli_endpoint = socket.socket(socket.AF_INET, socket.SOCK_STREAM)   # Criar socket (endpoint da ligação com o Server)

        self.channel = None                                                     # Canal em que o Cliente está neste momento



    def connect(self):
        """ Connect to chat server and setup stdin flags """

        # set sys.stdin non-blocking
        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)
        
        self.sel = selectors.DefaultSelector()                                      # Criar o selector
        self.sel.register(self.cli_endpoint, selectors.EVENT_READ, self.receive)    # Registar evento: Chama a função accept() quando há um evento de read no file descriptor self.cli_endpoint (número inteiro que identifica o socket no UNIX). A função receive() recebe como argumentos "self" e o primeiro argumento da função register (i.e., o file descriptor do socket do cliente)
        self.sel.register(sys.stdin, selectors.EVENT_READ, self.send)               # Registar evento de READ para quando existe um input do Cliente, para que assim a função send() seja executada
        
        self.cli_endpoint.connect(self.client_address)                              # Conectar ao socket
        self.cli_endpoint.setblocking(False)                                        # Não bloquear em I/O

        registerMessage = CDProto.register(self.name)                               # Registar um novo Cliente, obtendo uma mensagem (objeto) JSON com o username para poder ser enviada para o Server (i.e., de modo a respeitar o protocolo)
        CDProto.send_msg(self.cli_endpoint, registerMessage)                        # Enviar a 1ª mensagem para o Server
        logging.debug('Sent: "%s"', registerMessage)                                # Escrevê-la no ficheiro .log
        print(f"Connection to Server established.")
        


    def receive(self, sock):
        """ Lê novos dados vindos do server """

        server_data = CDProto.recv_msg(sock)                    # Receber os dados
        if not server_data:
            print("Connection to server ended.")
            exit(0)
        print(f"{server_data.message}", end="")                 # Imprimir os dados vindos do Server
        logging.debug('Received: "%s"', server_data)            # Escrevê-los também no ficheiro .log



    def send(self, stdin):
        """ Envia dados de input do Cliente para o server """
        messageToSend = stdin.read()                            # Ler o input do Cliente
        if messageToSend[0:5] == "/join":                       # Verificar se os primeiros caracteres constituem o comando "/join"
            newChannel = messageToSend[6:].strip()              # Se sim, extrair o canal da mensagem
            joinMessage = CDProto.join(newChannel)              # Criar uma mensagem JSON com o canal para poder ser enviada para o Server (i.e., de modo a respeitar o protocolo)
            CDProto.send_msg(self.cli_endpoint, joinMessage)    # Enviar a mensagem
            
            logging.debug('Sent: "%s"', joinMessage)            # Imprimir o pedido JSON "/join" no ficheiro .log

            self.channel = newChannel                           # Atualizar o canal onde o Cliente se encontra    
            print(f"Client '{self.name}' joinned channel '{newChannel}'.")

        elif messageToSend == "exit":                           # Se a mensagem a enviar for de "exit"
            logging.debug('Sent: "%s"', messageToSend)          # Escrever no ficheiro .log
            self.sel.unregister(self.cli_endpoint)              # Remover o registo do socket deste Cliente no selector
            self.cli_endpoint.close()                           # Fechar o socket
            print(f"Client '{self.name}' terminated connection.")
            sys.exit(0)

        else:
            textMessage = CDProto.message(messageToSend, self.channel)  # Se não for nem um nem outro, significa que é de texto, por converto-a numa mensagem JSON válida
            CDProto.send_msg(self.cli_endpoint, textMessage)            # Envio-a para o Server

            logging.debug('Sent: "%s"', textMessage)                    # E imprimo-a no ficheiro .log

    

    def loop(self):
        """Loop indefinetely."""

        while True:
            sys.stdout.write('')
            sys.stdout.flush()
            for k, mask in self.sel.select():
                callback = k.data
                callback(k.fileobj)
     
