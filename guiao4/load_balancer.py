# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse
import time

# configure logger output format
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None


# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):  
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    """This policy is used to forward all requests to the first server in the list."""
    def __init__(self, servers):
        self.servers = servers  

    def select_server(self):
        # simply return the first server in the list
        return self.servers[0]

    def update(self, *arg):
        pass


# round robin policy
class RoundRobin:
    """This policy is used to forward requests to servers in a round-robin fashion.
    It is implemented by keeping track of the current server and incrementing it
    by one each time a request is made."""
    def __init__(self, servers):
        self.servers = servers
        # current server index
        self.current = 0 

    def select_server(self):
        # get current server
        server = self.servers[self.current] 
        # increment current server index
        # if it reaches the end of the list, reset it to 0 (% len(self.servers))
        self.current = (self.current + 1) % len(self.servers) 
        return server
    
    def update(self, *arg):
        pass


# least connections policy
class LeastConnections:
    """This policy is used to forward requests to the server with the least number of connections.
    It is implemented by keeping track of the number of connections to each server and selecting
    the server with the least number of connections."""
    def __init__(self, servers):
        self.servers = servers
        # dictionary to keep track of the number of connections to each server
        # initially all servers have 0 connections
        self.connections = {server: 0 for server in servers}

    def select_server(self):
        # find the server with the least number of connections
        server = min(self.connections, key=self.connections.get)
        # increment the number of connections to the selected server
        # now the server has one more connection
        self.connections[server] += 1
        return server

    def update(self, *arg):
        # decrement the number of connections to the server
        if self.connections[arg[0]] > 0:
            self.connections[arg[0]] -= 1

# least response time
class LeastResponseTime:
    """This policy is used to forward requests to the server with the least response time.
    It is implemented by keeping track of the response time of each server and selecting
    the server with the least response time."""
    def __init__(self, servers):
        self.servers = servers
        # dictionary to keep track of the response time of each server
        # initially all servers have 0 response time
        self.response_time = {server: 0 for server in servers}
        # dictionary to keep track of the average response time of each server
        # initially all servers have 0 average response time
        self.avg_response_time = {server: 0 for server in servers}
        # dictionary to keep track of the past response time of each server
        # initially all servers begin with an empty list of past response times
        self.past_response_time = {server: [] for server in servers}
        # index of current server
        self.current = -1

    def select_server(self):
        # find the server with the least response time
        server = min(self.avg_response_time.values())
        
        for i in range(len(self.servers)):
            # increment the current server index
            # if it reaches the end of the list, reset it to -1
            if self.current + 1 == len(self.servers):
                self.current = -1
            self.current += 1
            # if the server with the least response time is found break the loop
            if self.avg_response_time[self.servers[self.current]] == server:
                break
        # start time of the current server
        self.response_time[server] = time.time()
        return self.servers[self.current]
        

    def update(self, *arg):
        # update the response time of the server
        # by calculating the difference between the current time and the start time
        response_time = time.time() - self.response_time[arg[0]]
        self.response_time[arg[0]] = response_time
        # add the response time to the list of past response times
        self.past_response_time[arg[0]].append(response_time)
        # calculate the average response time of the server
        self.avg_response_time[arg[0]] = sum(self.past_response_time[arg[0]]) / len(self.past_response_time[arg[0]])


POLICIES = {
    "N2One": N2One,
    "RoundRobin": RoundRobin,
    "LeastConnections": LeastConnections,
    "LeastResponseTime": LeastResponseTime
}

class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}

    def add(self, client_sock, upstream_server):
        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        sel.register(upstream_sock, selectors.EVENT_READ, read)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] =  upstream_sock

    def delete(self, sock):
        paired_sock = self.get_sock(sock)
        sel.unregister(sock)
        sock.close()
        sel.unregister(paired_sock)
        paired_sock.close()
        if sock in self.map:
            self.map.pop(sock)
        else:
            self.map.pop(paired_sock)

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ())) 

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    mapper.add(client, policy.select_server())

def read(conn,mask):
    data = conn.recv(4096)
    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        mapper.get_sock(conn).send(data)


def main(addr, servers, policy_class):
    global policy
    global mapper

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = policy_class(servers)
    mapper = SocketMapper(policy)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select(timeout=1)
            for key, mask in events:
                if(key.fileobj.fileno()>0):
                    callback = key.data
                    callback(key.fileobj, mask)
                
    except Exception as err:
        logger.error(err)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-a', dest='policy', choices=POLICIES)
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = [('localhost', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers, POLICIES[args.policy])
