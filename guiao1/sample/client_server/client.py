##!/usr/bin/env python

import socket

HOST = '127.0.0.1'    # The remote host
PORT = 50007              # The same port as used by the server
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    ian = input("Envia algo!\n")
    s.sendall(ian.encode())
    data = s.recv(1024)
print('Received', repr(data))
