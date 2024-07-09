"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from typing import Any

import socket
import json
import pickle
import xml.etree.ElementTree as ET


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        
        self.topic = topic
        self.type = _type
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(("localhost", 5000))

    def push(self, value):
        """Sends data to broker."""
        pass

    def pull(self) -> (str, Any): # type: ignore
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""
        pass

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        pass

    def cancel(self):
        """Cancel subscription."""
        pass


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        
        if _type == MiddlewareType.CONSUMER:
            data = {"command": "subscribe", "topic": topic}
            message = json.dumps(data).encode('utf-8')
            message_size = len(message).to_bytes(2, 'big')
            message_serializer = (0).to_bytes(1, 'big')
            self.sock.send(message_size + message_serializer + message)
    
    
    def push(self, value):
        """Sends data to broker."""
        data = {"command": "publish", "topic": self.topic, "message": value}
        message = json.dumps(data).encode('utf-8')
        message_size = len(message).to_bytes(2, 'big')
        message_serializer = (0).to_bytes(1, 'big')

        #print(f"Pacote: {int.from_bytes(message_size, 'big')} + {int.from_bytes(message_serializer, 'big')} + {data}")

        self.sock.send(message_size + message_serializer + message)
        
    
    def pull(self) -> (str, Any): # type: ignore
        """Receives (topic, data) from broker.
        
        Should BLOCK the consumer!"""
        message_size = int.from_bytes(self.sock.recv(2), 'big')
        #print(f"Tamanho da mensagem JSON: {message_size}\n")
        if message_size != 0:
            message_serializer = int.from_bytes(self.sock.recv(1), 'big')
            message = self.sock.recv(message_size)
            
            #print(f"Mensagem JSON recebida: {json.loads(message)}\n")
            return json.loads(message)["topic"], json.loads(message)["message"]
        
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        
        """
        data = {"command": "list"}
        message = json.dumps(data).encode('utf-8')
        message_size = len(message).to_bytes(2, 'big')
        self.sock.send(message_size + message)
        
        message_size = int.from_bytes(self.sock.recv(2), 'big')
        message = self.sock.recv(message_size)
        
        callback(json.loads(message))
        """
        pass

    def cancel(self):
        """Cancel subscription."""
        data = {"command": "cancel", "topic": self.topic}
        message = json.dumps(data).encode('utf-8')
        message_size = len(message).to_bytes(2, 'big')
        message_serializer = (0).to_bytes(1, 'big')
        self.sock.send(message_size + message_serializer + message)


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        
        if _type == MiddlewareType.CONSUMER:
            root = ET.Element("data")
            command = ET.SubElement(root, "command")
            command.text = "subscribe"
            topic = ET.SubElement(root, "topic")
            topic.text = topic
            message = ET.tostring(root)
            message_size = len(message).to_bytes(2, 'big')
            message_serializer = (1).to_bytes(1, 'big')
            self.sock.send(message_size + message_serializer + message)
    
    def push(self, value):
        """Sends data to broker."""
        message_serializer = (1).to_bytes(1, 'big')
        root = ET.Element('root')
        ET.SubElement(root, 'command').set("value", "publish")
        ET.SubElement(root, 'topic').set("value", self.topic)
        ET.SubElement(root, 'message').set("value", str(value))
        message = ET.tostring(root)

        message_size = len(message).to_bytes(2, "big")
        self.sock.send(message_size + message_serializer + message)
        
    def pull(self) -> (str, Any): # type: ignore
        """Receives (topic, data) from broker.
        
        Should BLOCK the consumer!"""
        message_size = int.from_bytes(self.sock.recv(2), 'big')
        #print(f"Tamanho da mensagem XML: {message_size}\n")
        if message_size != 0:
            message_serializer = int.from_bytes(self.sock.recv(1), 'big')
            message = self.sock.recv(message_size)
            root = ET.fromstring(message)

            #print(f"Mensagem XML recebida: {root.find('message').text}\n")
            return root.find("topic").text, root.find("message").text
    
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        pass
    
    def cancel(self):
        """Cancel subscription."""
        root = ET.Element("data")
        command = ET.SubElement(root, "command")
        command.text = "cancel"
        topic = ET.SubElement(root, "topic")
        topic.text = self.topic
        message = ET.tostring(root)
        message_size = len(message).to_bytes(2, 'big')
        message_serializer = (1).to_bytes(1, 'big')
        self.sock.send(message_size + message_serializer + message)


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        
        if _type == MiddlewareType.CONSUMER:
            data = {"command": "subscribe", "topic": topic}
            message = pickle.dumps(data)
            message_size = len(message).to_bytes(2, 'big')
            message_serializer = (2).to_bytes(1, 'big')
            self.sock.send(message_size + message_serializer + message)
    
    def push(self, value):
        """Sends data to broker."""
        data = {"command": "publish", "topic": self.topic, "message": value}
        message = pickle.dumps(data)
        message_size = len(message).to_bytes(2, 'big')
        message_serializer = (2).to_bytes(1, 'big')
        self.sock.send(message_size + message_serializer + message)
        pass

    def pull(self) -> (str, Any): # type: ignore
        """Receives (topic, data) from broker.
        
        Should BLOCK the consumer!"""
        message_size = int.from_bytes(self.sock.recv(2), 'big')
        #print(f"Tamanho da mensagem PICKLE: {message_size}\n")
        if message_size != 0:
            message_serializer = int.from_bytes(self.sock.recv(1), 'big')
            message = self.sock.recv(message_size)
            
            #print(f"Mensagem PICKLE recebida: {pickle.loads(message)}\n")
            return pickle.loads(message)["topic"], pickle.loads(message)["message"]
    
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        pass
    
    def cancel(self):
        """Cancel subscription."""
        data = {"command": "cancel", "topic": self.topic}
        message = pickle.dumps(data)
        message_size = len(message).to_bytes(2, 'big')
        message_serializer = (2).to_bytes(1, 'big')
        self.sock.send(message_size + message_serializer + message)