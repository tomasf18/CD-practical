"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket



''' ------- Classe e Subclasses para cada tipo de mensagem ------- '''
class Message:
    """Message Type."""
    def __init__(self, command):
        self.command = command;         # Comando da mensagem



class JoinMessage(Message):
    """Message to join a chat channel."""

    def __init__(self, channel):
        super().__init__("join")        # Como a classe é Join, o arg do construtor da classe-mãe é "join"
        self.channel = channel          # Canal para o onde o Cliente quer entrar

    def __repr__(self) -> str:          # Método de representação do objeto
        return json.dumps( {
            "command": "join",
            "channel": self.channel
        })



class RegisterMessage(Message):
    """Message to register username in the server."""
    def __init__(self, username):
        super().__init__("register")    # Como a classe é Register, o arg do construtor da classe-mãe é "register"
        self.username = username        # username do Cliente

    # Representation of the object
    def __repr__(self) -> str:          # Método de representação do objeto
        return json.dumps( {
            "command": "register",
            "user": self.username
        })    

    

class TextMessage(Message):
    """Message to chat with other clients."""
    def __init__(self, message, channel=None, timestamp=None):  
        super().__init__("message")                             # Como a classe é Text, o arg do construtor da classe-mãe é "message"
        self.message = message                                  # Conteúdo da mensagem
        self.channel = channel                                  # Canal para onde o Cliente quer enviar a mensagem
        self.timestamp = timestamp                              # Tempo da mensagem desde 1 de Janeiro de 1970 

    def __repr__(self) -> str:                                  # Método de representação do objeto
        if self.channel == None:                                # Caso o canal não venha na mensagem, a mesma é enviada para o canal default (main)
            return json.dumps( {                                
                "command": "message",
                "message": self.message,
                "ts": self.timestamp
            })
        else:
            return json.dumps( {
                "command": "message",
                "message": self.message,
                "channel": self.channel,
                "ts": self.timestamp
            })




''' ------- Definição do protocolo a ser cumprido ------- '''
class CDProto:
    """Computação Distribuida Protocol."""


    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        return RegisterMessage(username)


    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage(channel)


    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        return TextMessage(message, channel, int(datetime.now().timestamp()))   # Criar já com o timestamp da altura do envio da mensagem


    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""

        message = str(msg)
        message_size = len(message).to_bytes(2, 'big')              # Obter o tamanho da mensagem e representá-lo em 2 bytes
        connection.send(message_size + message.encode('utf-8'))     # Enviar o conteúdo já com o tamanho incluído


    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""

        message_size = int.from_bytes(connection.recv(2), 'big')    # Obter o tamanho da mensagem através dos primeiros 2 bytes
        
        if message_size == 0:
            return None
        
        message = connection.recv(message_size).decode('utf-8')     # Obter o conteúdo da mensagem

        try:
            message_to_json = json.loads(message)                   # Tentar converter em json
        except json.JSONDecodeError:
            raise CDProtoBadFormat(message)

        if "command" not in message_to_json.keys():                 # Verifica se existe um comando (necessário) na mensagem
            raise CDProtoBadFormat(message_to_json)
        
        command = message_to_json["command"]

        # ------ Caso o comando seja "join" ------ # 
        if command == "join":
            if "channel" not in message_to_json.keys():
                raise CDProtoBadFormat(message_to_json)  
            return CDProto.join(message_to_json["channel"]) 
        # ------ Caso o comando seja "register" ------ # 
        elif command == "register":
            if "user" not in message_to_json.keys():
                raise CDProtoBadFormat(message_to_json)  
            return CDProto.register(message_to_json["user"]) 
        # ------ Caso o comando seja "message" ------ #  
        elif command == "message":
            if "message" not in message_to_json.keys():
                raise CDProtoBadFormat(message_to_json)  
        
            if "channel" in message_to_json.keys():
                return CDProto.message(message_to_json["message"], message_to_json["channel"])
            else:
                return CDProto.message(message_to_json["message"]) 
            
        return None
  


''' ------- Definição das exceções ao protocolo criado ------- '''
class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
