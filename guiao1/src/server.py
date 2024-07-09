"""CD Chat server program."""
import logging
import socket
import selectors
from .protocol import CDProto, CDProtoBadFormat

logging.basicConfig(filename="server.log", level=logging.DEBUG)


class Server:
    """Chat Server process."""

    def __init__(self):
        """ Initializes chat client """

        self.sel = selectors.DefaultSelector()                              # Criar o selector
        
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)     # Criar socket
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)   # Reutilizar porta
        self.server.bind(('localhost', 9010))                               # Criar vínculo
        self.server.listen(100)                                             # Pronto a ouvir pedidos de novas ligações

        self.sel.register(self.server, selectors.EVENT_READ, self.accept)   # Registar o evento READ para quando chegar um novo pedido executar a função accept()

        self.channels = {"#main": []}                                       # Canais disponíveis com a lista dos clientes conectados em cada um {"#canal": [sockets_conectados]}
        self.users = {}                                                     # Utilizadores ativos {client_socket: "username"}


    def accept(self, sock, mask):
        """ Aceitar ligações com novos clientes """

        cli_serv_communic, cli_addr = sock.accept()                             # Aceitar uma ligação
        print(f"Connection established with '{cli_addr}'.")  
        
        cli_serv_communic.setblocking(False)                                    # Não quero que bloqueie no I/O

        self.sel.register(cli_serv_communic, selectors.EVENT_READ, self.read)   # Registar o evento READ para quando chegar uma nova mensagem executar a função read()

        self.channels["#main"].append(cli_serv_communic)                        # Adicionar o cliente à lista de clientes conectados (primeiramente só existe o canal "#main")


    def read(self, cli_serv_communic, mask):
        """ Lê novos dados na comunicação e reencaminha-os """

        try:
            cli_data = CDProto.recv_msg(cli_serv_communic)                      # Verifico se os dados que são recebidos são válidos (segundo o protocolo)
        except CDProtoBadFormat:
            print("SERVER: Bad format.")
            exit(1) 

        logging.debug('Received: "%s"', cli_data)                               # Escrever dados recebidos no ficheiro .log    

        print(cli_data)

        if cli_data:
            # ------- Se o comando recebido for de registo ------- #
            if cli_data.command == "register":
                print(f"Client '{cli_data.username}' registered.")
                self.users[cli_serv_communic] = cli_data.username               # Registo um novo utilizador

            # ------- Se o comando recebido for de "/join" ------- #
            elif cli_data.command == "join":
                if cli_data.channel in self.channels.keys():                    # Adiciono-o ao canal
                    self.channels[cli_data.channel].append(cli_serv_communic)   
                else:
                    self.channels[cli_data.channel] = [cli_serv_communic]
                #print(self.channels)
                print(f"Client '{self.users[cli_serv_communic]}' joinned channel '{cli_data.channel}'.")   

            # ------- Se o comando recebido for de mensagem de texto ------- #
            elif cli_data.command == "message":
                if cli_data.message[:-1] == "exit":                                          # Caso seja um pedido de "exit"
                    print(f"Client '{self.users[cli_serv_communic]}' disconnected.")        
                    self.users.pop(cli_serv_communic)                                   # Por isso removo-o do array users
                    for channel in self.channels.keys():
                        if cli_serv_communic in self.channels[channel]:                 # Bem como de todos os canais em que se encontra 
                            self.channels[channel].remove(cli_serv_communic)
                    self.sel.unregister(cli_serv_communic)                              # Tiro o seu registo do selector
                    cli_serv_communic.close()  
                else:    
                    if cli_data.channel:                                                    # Se inseriu o canal
                        for cli_socket in self.channels[cli_data.channel]:                  # Enviar para todos nesse canal
                            CDProto.send_msg(cli_socket, cli_data)
                            logging.debug('Sent: "%s"', cli_data)                           # Escrever dados enviados no ficheiro .log
                        print(f"Sendig to all of those present in '{cli_data.channel}'.")
                    else:                                                                   # Senão
                        for cli_socket in self.channels["#main"]:                           # Enviar para todos no canal padrão: "#main"
                            CDProto.send_msg(cli_socket, cli_data)
                            logging.debug('Sent: "%s"', cli_data)                           # Escrever dados enviados no ficheiro .log
                        print(f"Sending to all of those present in '#main'.")
        else:                                                                   # Se não for nenhum dos casos anteriores, significa que o Cliente introduziu "CTRL+C" para terminar o processo
            print(f"Client '{self.users[cli_serv_communic]}' disconnected.")        
            self.users.pop(cli_serv_communic)                                   # Por isso removo-o do array users
            for channel in self.channels.keys():
                if cli_serv_communic in self.channels[channel]:                 # Bem como de todos os canais em que se encontra 
                    self.channels[channel].remove(cli_serv_communic)
            self.sel.unregister(cli_serv_communic)                              # Tiro o seu registo do selector
            cli_serv_communic.close()                                           # E fecho o socket


    # Selector
    def loop(self):
        """ Loop indefinetely """

        while True:
            events = self.sel.select()
            for (key, mask) in events:
                callback = key.data
                callback(key.fileobj, mask)