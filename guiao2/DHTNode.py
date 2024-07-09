""" Chord DHT node implementation. """
import socket
import threading
import logging
import pickle
from utils import dht_hash, contains


class FingerTable:
    """Finger Table."""

    def __init__(self, node_id, node_addr, m_bits=10):
        """ Initialize Finger Table."""
        self.node_id = node_id          # ID do nó no anel
        self.node_addr = node_addr      # Endereço do nó
        self.m_bits = m_bits            # Número de bits a serem utilizados para o espaço de endereçamento do anel
        
        # O Chord Ring terá, no máximo, 2^m_bits nós, cujos id's variam de 0 a 2^m_bits-1
        # A finger table de cada nó terá, no máximo, log2(m_bits) entradas
        self.finger_table = []
        self.finger_keyIdx = []

        # Refactor com fill()
        for i in range(self.m_bits):    # "Each node 'n' mantains a routing table with up to 'm' entries"
            self.finger_table.append((node_id, node_addr))      # "The i(th) entry in the table at node 'n' contains the identuty of the first node 's' that succeeds 'n' by at least 2^i-1"
            # "A finger table entry includes both the Chord identifier and the IP address of the relevant node"
            #keyIdx = (self.node_id + 2**i) % (2**self.m_bits)   # Mapear para o anel (N8 + i, Nó onde está armazenado o objeto com id=8+i)
            #self.finger_keyIdx.append((i+1, keyIdx))              # "i+1" porque começa em 1
        pass


    '''
    Node 10 finger_keyIdx    Node 10 finger table
        +-----------+    
      1 |10 + 2¹⁻¹  |        0(node_id, node_addr) -> Where node_addr = (host, port)
      2 |10 + 2²⁻¹  |        1(node_id, node_addr)
      3 |10 + 2³⁻¹  |        2(node_id, node_addr)
      4 |10 + 2⁴⁻¹  |        3(node_id, node_addr)
      5 |10 + 2⁵⁻¹  |        4(EU, node_addr)
      6 |10 + 2⁶⁻¹  |        5(node_id, node_addr)
      7 |10 + 2⁷⁻¹  |        6(node_id, node_addr)
      8 |10 + 2⁸⁻¹  |        7(node_id, node_addr)
      9 |10 + 2⁹⁻¹  |        8(node_id, node_addr)
     10 |10 + 2¹⁰⁻¹ |        9(node_id, node_addr)
        +-----------+
    '''

    def fill(self, node_id, node_addr):
        """ Fill all entries of finger_table with node_id, node_addr."""
        for i in range(self.m_bits):
            self.finger_table[i] = (node_id, node_addr)
        pass

    def update(self, index, node_id, node_addr):
        """Update index of table with node_id and node_addr."""
        self.finger_table[index-1] = (node_id, node_addr)
        pass

    def find(self, identification):
        """ Get node address of closest preceding node (in finger table) of identification. """ # é o imediato predecessor porque não conseguimos, apenas através da nossa finguer table, concluir aquele que está logo a seguir na fingertable depois do predecessor encontrado é realmente o sucessor, visto que o salto dado é grande e podem ter sido saltados nós (Logo, a unica condição que nos permite concluir se um nó é sucessor de uma key, é se a key está entre nós e o nosso sucessor)
        # Pág 5 do paper do Chord (começar no fim -> certo!)
        for i in range(self.m_bits-1, -1, -1):
            if contains(self.finger_table[i][0], self.node_id, identification):
                return self.finger_table[i][1]
        return self.finger_table[-1][1]

    def refresh(self):
        """ Retrieve finger table entries requiring refresh."""
        refreshable_entries = []

        # Pág 6 do paper do Chord
        for i in range(self.m_bits):
            index = i + 1
            key = (self.node_id + 2**i) % (2 ** self.m_bits)
            node_addr = self.finger_table[i][1]
            refreshable_entries.append((index, key, node_addr))

        return refreshable_entries           

    def getIdxFromId(self, id):
        for i in range(self.m_bits):
            # value of the finger table
            value = (self.node_id + 2 ** i) % (2 ** self.m_bits)  # n + 2^i
            # Adding 2 ** i to self.node_id gives us the ID of the node that is 2^i places ahead of the current node
            if contains(self.node_id, value, id):
                return i + 1 

    def __repr__(self):
        return str(self.as_list)

    @property
    def as_list(self):
        """return the finger table as a list of tuples: (identifier, (host, port)).
        NOTE: list index 0 corresponds to finger_table index 1
        """
           
        return self.finger_table;

class DHTNode(threading.Thread):
    """ DHT Node Agent. """

    def __init__(self, address, dht_address=None, timeout=3):
        """Constructor

        Parameters:
            address: self's address
            dht_address: address of a node in the DHT
            timeout: impacts how often stabilize algorithm is carried out
        """
        threading.Thread.__init__(self)
        self.done = False
        self.identification = dht_hash(address.__str__())
        self.addr = address  # My address
        self.dht_address = dht_address  # Address of the initial Node
        if dht_address is None:
            self.inside_dht = True
            # I'm my own successor
            self.successor_id = self.identification
            self.successor_addr = address
            self.predecessor_id = None
            self.predecessor_addr = None
        else:
            self.inside_dht = False
            self.successor_id = None
            self.successor_addr = None
            self.predecessor_id = None
            self.predecessor_addr = None

        # --------------------- #
        #TODO create finger_table
        self.finger_table = FingerTable(self.identification, self.addr)    
        # --------------------- #

        self.keystore = {}  # Where all data is stored
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP!
        self.socket.settimeout(timeout)
        self.logger = logging.getLogger("Node {}".format(self.identification))

    def send(self, address, msg):
        """ Send msg to address. """
        payload = pickle.dumps(msg)
        self.socket.sendto(payload, address)

    def recv(self):
        """ Retrieve msg payload and from address."""
        try:
            payload, addr = self.socket.recvfrom(1024)
        except socket.timeout:
            return None, None

        if len(payload) == 0:
            return None, addr
        return payload, addr

    def node_join(self, args):
        """Process JOIN_REQ message.

        Parameters:
            args (dict): addr and id of the node trying to join
        """

        self.logger.debug("Node join: %s", args)
        addr = args["addr"]
        identification = args["id"]
        if self.identification == self.successor_id:  # I'm the only node in the DHT
            # No caso do meu ID ainda ser igual ao ID do meu sucessor (i.e., ainda sou o 
            # único na rede, logo sou o sucessor de mim mesmo), então o nó que dá JOIN passa 
            # a ser o meu sucessor e predecessor (2º nó na rede, a seguir a mim)
            self.successor_id = identification
            self.successor_addr = addr
            # --------------------- #
            #TODO update finger table
            self.finger_table.fill(identification, addr) # Agora tenho um sucessor
            # --------------------- #
            args = {"successor_id": self.identification, "successor_addr": self.addr}
            self.send(addr, {"method": "JOIN_REP", "args": args})

        elif contains(self.identification, self.successor_id, identification):  # No caso de, antes do nóvo nó dar JOIN, eu já não for o único
            args = {        # Envio esta mensagem para o novo nó, informando-o que o seu sucessor é o meu antigo sucessor
                "successor_id": self.successor_id,
                "successor_addr": self.successor_addr,
            }
            # Atualizo o meu sucessor para o nó que entrou
            self.successor_id = identification
            self.successor_addr = addr
            # --------------------- #
            #TODO update finger table
            self.finger_table.fill(identification, addr) # Agora tenho um sucessor
            # --------------------- #
            self.send(addr, {"method": "JOIN_REP", "args": args})
        else:
            # Se eu não sou o primeiro nó e o nó que quer entrar não é o meu sucessor, então tenho de enviar o pedido para o meu sucessor
            self.logger.debug("Find Successor(%d)", args["id"])
            self.send(self.successor_addr, {"method": "JOIN_REQ", "args": args})
        self.logger.info(self)

    def get_successor(self, args):
        """Process SUCCESSOR message.

        Parameters:
            args (dict): addr and id of the node asking
        """

        self.logger.debug("Get successor: %s", args)
        # --------------------- #
        #TODO Implement processing of SUCCESSOR message

        address = args["from"]
        id = args["id"]
        
        if contains(self.identification, self.successor_id, id):
            self.send(address, {"method": "SUCCESSOR_REP", "args": {"req_id" : id, "successor_id": self.successor_id, "successor_addr": self.successor_addr}})
        else:
            # Caso eu não tenha a resposta para "address", envio este pedido para o meu sucessor, sendo que identifico o pedido com a origem (from) "address"
            closest_preceding_node = self.finger_table.find(id)
            self.send(self.successor_addr, {"method": "SUCCESSOR", "args": {"id": id, "from": address}})
        # --------------------- #
                
    def notify(self, args):
        """Process NOTIFY message.
            Updates predecessor pointers.

        Parameters:
            args (dict): id and addr of the predecessor node
        """

        self.logger.debug("Notify: %s", args)
        # O nó que me notificou é quem me quer informar que é ele o meu predecessor, e eu tenho de verificar se eu já estou atualizado disso ou se ainda me tenho de atualizar (i.e., se o nó que enviou a mensagem está entre mim e o o predecessor que eu pensava que tinha)
        if self.predecessor_id is None or contains(self.predecessor_id, self.identification, args["predecessor_id"]):      
            self.predecessor_id = args["predecessor_id"]
            self.predecessor_addr = args["predecessor_addr"]
        self.logger.info(self)

    def stabilize(self, from_id, addr):
    # from_id manda ao seu predecessor o stabilize
        """Process STABILIZE protocol.
            Updates all successor pointers!!!!!!!!!!
        Parameters:
            from_id: id of the predecessor of node with address addr    -> É o ID do predecessor, que quer saber o predecessor do seu sucessor (para ver se continua a ser ele próprio, i.e, para ver se o nó que entrou não entrou entre ele e o seu sucessor) 
            addr: address of the node sending stabilize message         -> Ou seja, o predecessofr do meu sucessor quer saber se o seu sucessor continua a ser o seu sucessor
        """

        self.logger.debug("Stabilize: %s %s", from_id, addr)
        if from_id is not None and contains(self.identification, self.successor_id, from_id):
            # Update our successor
            self.successor_id = from_id
            self.successor_addr = addr
            # --------------------- #
            #TODO update finger table Só a primeira entrada???
            self.finger_table.update(1, self.successor_id, self.successor_addr)
            # --------------------- #

        # notify successor of our existence, so it can update its predecessor record, if needed
        args = {"predecessor_id": self.identification, "predecessor_addr": self.addr}
        self.send(self.successor_addr, {"method": "NOTIFY", "args": args})

        # --------------------- #
        # TODO refresh finger_table
        refreshable_entries = self.finger_table.refresh()
        
        for i in range(len(refreshable_entries)):
            # Não é preciso um valor de retorno para aqui?
            self.get_successor({"id": refreshable_entries[i][1], "from": self.addr})
        # --------------------- #

    def put(self, key, value, address):
        """Store value in DHT.

        Parameters:
        key: key of the data
        value: data to be stored
        address: address where to send ack/nack
        """
        key_hash = dht_hash(key)
        self.logger.debug("Put: %s %s", key, key_hash)

        if contains(self.identification, self.successor_id, key_hash):
            # Se a chave está entre mim e o meu sucessor, então o meu sucessor é o responsável por guardar a chave
            self.send(self.successor_addr, {"method": "PUT", "args": {"key": key, "value": value, "from": address}})
        elif contains(self.predecessor_id, self.identification, key_hash):
            # Se a chave está entre o meu predecessor e eu, então eu sou o responsável por guardar a chave
            if key not in self.keystore:
                self.keystore[key] = value
                self.send(address, {"method": "ACK"})
                self.logger.debug("Stored: %s", self.keystore)
            else:
                self.send(address, {"method": "NACK"})
        else:
            # Se a chave não está entre mim e o meu sucessor, então tenho de enviar a mensagem para o nó na minha FT que está imediatamente antes da chave
            self.send(self.finger_table.find(key_hash), {"method": "PUT", "args": {"key": key, "value": value, "from": address}})


    def get(self, key, address):
        """Retrieve value from DHT.

        Parameters:
        key: key of the data
        address: address where to send ack/nack
        """
        key_hash = dht_hash(key)
        self.logger.debug("Get: %s %s", key, key_hash)

        if contains(self.identification, self.successor_id, key_hash):
            # Se a chave está entre mim e o meu sucessor, então o meu sucessor é o responsável por guardar e devolver a chave
            self.send(self.successor_addr, {"method": "GET", "args": {"key": key, "from": address}})
        elif contains(self.predecessor_id, self.identification, key_hash):
            # Se a chave está entre o meu predecessor e eu, então eu sou o responsável por guardar e devolver a chave
            if (key in self.keystore):
                self.send(address, {'method': 'ACK', 'args': self.keystore[key]})
            else:
                self.send(address, {"method": "NACK"})
        else:
            # Se a chave não está entre mim e o meu sucessor, então tenho de enviar a mensagem para o nó na minha FT que está imediatamente antes da chave
            self.send(self.finger_table.find(key_hash), {"method": "GET", "args": {"key": key, "from": address}})


    def run(self):
        self.socket.bind(self.addr)

        # Loop untiln joining the DHT
        while not self.inside_dht:
            join_msg = {
                "method": "JOIN_REQ",
                "args": {"addr": self.addr, "id": self.identification},
            }
            self.send(self.dht_address, join_msg)
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.debug("O: %s", output)
                if output["method"] == "JOIN_REP":
                    args = output["args"]
                    self.successor_id = args["successor_id"]
                    self.successor_addr = args["successor_addr"]
                    # --------------------- #
                    #TODO fill finger table
                    self.finger_table.fill(self.successor_id, self.successor_addr)  # Preencher a minha tabela com o meu sucessor
                    # --------------------- #
                    self.inside_dht = True
                    self.logger.info(self)

        while not self.done:
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.info("O: %s", output)
                if output["method"] == "JOIN_REQ":
                    self.node_join(output["args"])
                elif output["method"] == "NOTIFY":
                    self.notify(output["args"])
                elif output["method"] == "PUT":
                    self.put(
                        output["args"]["key"],
                        output["args"]["value"],
                        output["args"].get("from", addr),
                    )
                elif output["method"] == "GET":
                    self.get(output["args"]["key"], output["args"].get("from", addr))
                elif output["method"] == "PREDECESSOR":
                    # Reply with predecessor id, visto que o objetivo é saber quem é o predecessor do nó sucessor, que, neste caso, como sou eu que recebo o pedido, sou eu
                    self.send(
                        addr, {"method": "STABILIZE", "args": self.predecessor_id}
                    )
                elif output["method"] == "SUCCESSOR":
                    # Reply with successor of id
                    self.get_successor(output["args"])
                elif output["method"] == "STABILIZE":
                    # Initiate stabilize protocol
                    self.stabilize(output["args"], addr)
                elif output["method"] == "SUCCESSOR_REP":
                    # --------------------- #
                    #TODO Implement processing of SUCCESSOR_REP
                    # Eu fiz um pedido para saber quem é o sucessor do objeto com id output["args"]["req_id"], e agora recebi a resposta. O que tenho de fazer com estes dados? Atualizar a minha FT!
                    id = output["args"]["req_id"]
                    index = self.finger_table.getIdxFromId(id)
                    succ_id = output["args"]["successor_id"]
                    succ_addr = output["args"]["successor_addr"]
                    self.finger_table.update(index, succ_id, succ_addr)

                    ''' Antes estava assim:
                    
                    id = output["args"]["req_id"]
                    index = self.finger_table.getIdxFromId(id)

                    ATENÇÃO: Mas como um SUCCESSOR_REP é resposta a um pedido que eu enviei para a rede para obter o sucessor do objeto com id 'req_id', então 'succ_id' e 'succ_addr' são os dados desse sucessor, E NÃO DO MEU SUCESSOR
                    
                    self.successor_id = output["args"]["successor_id"]
                    self.successor_addr = output["args"]["successor_addr"]
                    self.finger_table.update(index, self.successor_id, self.successor_addr)
                    '''
                    # --------------------- #
            else:  # timeout occurred, lets run the stabilize algorithm
                # Ask successor for predecessor, to start the stabilize process
                self.send(self.successor_addr, {"method": "PREDECESSOR"})

    def __str__(self):
        return "Node ID: {}; DHT: {}; Successor: {}; Predecessor: {}; FingerTable: {}".format(
            self.identification,
            self.inside_dht,
            self.successor_id,
            self.predecessor_id,
            self.finger_table,
        )

    def __repr__(self):
        return self.__str__()
