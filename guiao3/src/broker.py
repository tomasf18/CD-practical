"""Message Broker"""
import enum
import json
import pickle
import xml.etree.ElementTree as ET
import socket
import selectors
from typing import Dict, List, Any, Tuple

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000

        self.sel = selectors.DefaultSelector()

        self.broker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.broker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.broker_socket.bind((self._host, self._port))
        self.broker_socket.listen(20)

        self.sel.register(self.broker_socket, selectors.EVENT_READ, self.handle_new_conn)

        self.topics_values = dict()
        self.topics_subscriptions = dict()


    def handle_new_conn(self, cli_sock, mask):
        cli_broker_comm, cli_addr = cli_sock.accept()
        #print(f"Connection established with '{cli_addr}'")
        cli_broker_comm.setblocking(False)
        self.sel.register(cli_broker_comm, selectors.EVENT_READ, self.handle_client_requests)


    def handle_client_requests(self, cli_sock, mask):
        message_size = int.from_bytes(cli_sock.recv(2), 'big')

        if message_size == 0:
            for topic in self.topics_subscriptions.keys():
                self.unsubscribe(topic, cli_sock)
            self.sel.unregister(cli_sock)
            cli_sock.close()    
            return
        
        message_serializer = int.from_bytes(cli_sock.recv(1), 'big')
        message = cli_sock.recv(message_size)

        if message_serializer == Serializer.JSON.value:
            self.handle_json_message(cli_sock, message)
        elif message_serializer == Serializer.XML.value:
            self.handle_xml_message(cli_sock, message)
        elif message_serializer == Serializer.PICKLE.value:
            self.handle_pickle_message(cli_sock, message)
        else:
            print("Bad format: size (2B) | serializer (1B) | message")
            return None 


    def handle_pickle_message(self, cli_sock, message):
        try:
            message_pickle = pickle.loads(message)
            if not message_pickle:
                cli_sock.close()
                return
            #print(f"Handling PICKLE message: {message_pickle}\n")
        except:
            print("Bad format")
            return

        if "command" not in message_pickle.keys():
            print("No command!")
            return
        if "topic" not in message_pickle.keys():
            print("No topic!")
            return
        
        command = message_pickle["command"]
        if command == "subscribe":
            self.subscribe(message_pickle["topic"], cli_sock, Serializer.PICKLE)
        elif command == "publish":
            if "message" not in message_pickle.keys():
                print("No message!")
                return
            topic = message_pickle["topic"]
            #print(f"Topic: {topic} \n")
            message_content = message_pickle["message"]
            #print(f"Message: {message_content}\n")
            #print(f"Before put in topic '{topic}' the content '{str(message_content)}'\n")
            self.put_topic(topic, message_content)
            #print(f"After put in topic '{topic}' the content '{str(message_content)}'\n")
            #print(f"self.send_message({topic}, {message_content})\n")
            self.send_message(topic, message_content)
        elif command == "list":
            pass
        elif command == "cancel":
            self.unsubscribe(message_pickle["topic"], cli_sock)   


    def handle_json_message(self, cli_sock, message):
        try:
            message_json = json.loads(message.decode("utf-8"))
            if not message_json:
                print(">> one socket has left")
                cli_sock.close()
                return
            print(f"Handling JSON message: {json.dumps(message_json)}\n")
        except:
            print("Bad format: " + message)
            return

        if "command" not in message_json.keys():
            print("No command!")
            return
        if "topic" not in message_json.keys():
            print("No topic!")
            return
        
        command = message_json["command"]
        # print(f"Command: {command} \n")
        if command == "subscribe":
            self.subscribe(message_json["topic"], cli_sock, Serializer.JSON)

        elif command == "publish":
            if "message" not in message_json.keys():
                print("No message!")
                return
            topic = message_json["topic"]
            # print(f"Topic: {topic} \n")
            message_content = message_json["message"]
            # print(f"Message: {message_content}\n")
            # print(f"Before put in topic '{topic}' the content '{str(message_content)}'\n")
            self.put_topic(topic, message_content)
            # print(f"After put in topic '{topic}' the content '{str(message_content)}'\n")
            self.send_message(topic, message_content)
            
        elif command == "list":
            pass
        elif command == "cancel":
            self.unsubscribe(message_json["topic"], cli_sock)


    def handle_xml_message(self, cli_sock, message):
        try:
            message_xml = ET.fromstring(message)
            if not message_xml:
                cli_sock.close()
                return
            #print(f"Handling XML message: {message_xml}\n")
        except:
            print("Bad format XML")
            return
        
        command = message_xml.find("command")
        if command is None:
            print("No command!")
            return
        
        command = command.text
        if command == "subscribe":
            topic = message_xml.find("topic")
            if topic is None:
                print("No topic!")
                return
            self.subscribe(topic.text, cli_sock, Serializer.XML)
        elif command == "publish":
            topic = message_xml.find("topic")
            message_content = message_xml.find("message")
            if "message" == None:
                print("No message!")
                return
            if topic is None:
                print("No topic!")
                return
            self.put_topic(topic.text, message_content.text)
            self.send_message(topic.text, message_content.text)
        elif command == "list":
            pass
        elif command == "cancel":
            topic = message_xml.find("topic")
            if topic is None:
                print("No topic!")
                return
            self.unsubscribe(topic.text, cli_sock)
            

    def send_message(self, topic, message):
        """ Send message to all subscribers of topic."""
        if topic not in self.topics_subscriptions.keys():
            return
        
        msg = {"command": "publish", "topic": topic, "message": message}
        print(f"Sending message: {msg} \n")
        print(f"Subscriptions: {self.topics_subscriptions} \n")
        for t in self.topics_values:
            if t is not None and topic.startswith(t):
                for subscriber in self.list_subscriptions(t):
                    print(f"To Subscriber: {subscriber} \n")
                    conn = subscriber[0]
                    format = subscriber[1]
                    if format == Serializer.JSON:
                        self.send_message_JSON(msg, conn)
                    elif format == Serializer.XML:
                        self.send_message_XML(msg, conn)
                    elif format == Serializer.PICKLE:
                        self.send_message_PICKLE(msg, conn)
                    else:
                        print(f"Bad format in send_message(): format is {format} in {subscriber} \n")
                        return None  

    
    def send_message_PICKLE(self, msg, conn):
        encoded_msg = pickle.dumps(msg) 
        message_size = len(encoded_msg).to_bytes(2, 'big')
        message_serializer = (2).to_bytes(1, 'big')
        #print(f"Mensagem: {int.from_bytes(message_size, 'big')} + {int.from_bytes(message_serializer, 'big')} + {msg} para PICKLE\n")
        conn.send(message_size + message_serializer + encoded_msg) 


    def send_message_XML(self, msg, conn):
        root = ET.Element("data")
        t = ET.SubElement(root, "topic")
        t.text = t
        message = ET.SubElement(root, "message")
        message.text = message_serializer
        encoded_msg = ET.tostring(root)
        message_size = len(encoded_msg).to_bytes(2, 'big')
        message_serializer = (1).to_bytes(1, 'big')
        #print(f"Mensagem: {int.from_bytes(message_size, 'big')} + {int.from_bytes(message_serializer, 'big')} + {msg} para XML\n")
        conn.send(message_size + message_serializer + encoded_msg)  


    def send_message_JSON(self, msg, conn):
        encoded_msg = json.dumps(msg).encode("utf-8")
        message_size = len(encoded_msg).to_bytes(2, 'big')
        message_serializer = (0).to_bytes(1, 'big')
        #print(f"Mensagem: {int.from_bytes(message_size, 'big')} + {int.from_bytes(message_serializer, 'big')} + {msg} para JSON\n")
        conn.send(message_size + message_serializer + encoded_msg)


    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        topics_containing_values = []
        for topic in self.topics_values.keys():
            if self.topics_values[topic] != "":
                topics_containing_values.append(topic)
        return topics_containing_values


    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.topics_values.keys():
            return self.topics_values[topic]
        return None


    def put_topic(self, topic, value):
        """Store in topic the value."""
        if topic is None: 
            return
    
        #print(f"Putting {value} in '{topic}' \n")
        self.topics_values[topic] = value 
        if topic not in self.topics_subscriptions.keys():   
            self.topics_subscriptions[topic] = []


    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        if topic in self.topics_subscriptions.keys():
            return self.topics_subscriptions[topic]
        else:
            print(f"Topic '{topic}' does not exist.")
            return None


    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        #if topic is None:
        #    return

        if _format == Serializer.XML:
            print(f"Subscribing to topic '{topic}' in XML format\n")
            
        if topic not in self.topics_subscriptions.keys():
            self.topics_subscriptions[topic] = [(address, _format)]
            self.topics_values[topic] = ""
        else:
            self.topics_subscriptions[topic].append((address, _format))   

        if self.topics_values[topic] != "":
            msg = {"command": "publish", "topic": topic, "message": self.topics_values[topic]}
            if _format == Serializer.JSON:
                self.send_message_JSON(msg, address) 
            elif _format == Serializer.XML:
                self.send_message_XML(msg, address)
            elif _format == Serializer.PICKLE:
                self.send_message_PICKLE(msg, address)


    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        if topic is None:
            return
        
        if topic not in self.topics_subscriptions.keys():
            print("There is no such topic.")
        else:
            for t in self.topics_values.keys():
                if t is not None and t.startswith(topic):
                    for sub in self.topics_subscriptions[t]:
                        if sub[0] == address:
                            self.topics_subscriptions[t].remove(sub)


    def run(self):
        """Run until canceled."""

        while not self.canceled:
            events = self.sel.select()
            for (key, mask) in events:
                callback = key.data
                callback(key.fileobj, mask)
