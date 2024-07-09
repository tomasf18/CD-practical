#!/usr/bin/env python
import socket

HOST = 'localhost'  # ou o IP privado do meu computador (que é onde vai estar o server)
PORT = 9010         # Qualquer um entre 0-65536 exceto os conhecidos (ex.: 80 para HTTP)
# O PORT tem de ser o mesmo no server E no cliente

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Pretendo um canal de comunicação atravé de IPv4 e TCP (não quero perdas de pacotes, e quero saber quando envio/recebo algo com sucesso)

# Agora temos um socket, mas não sabemos para é que este "endpoint" de canal vai servir.
# Ou seja, não sabemos se vai ser um socket para conectar com algo ou para hospedar (host) alguma coisa.
# Como queremos que seja um server, temos de dar bind() (vinculá-lo) a um HOST e a um PORT
server.bind((HOST, PORT))   # Passa-se o tuplo (HOST, PORT)

# Agora precisamos de estar a preparados para "ouvir" novas ligações:
server.listen(5) # O 5 indica o número máximo de conexões que podem esperar por ser aceites, isto é, no máximo 5 conexões à espera do "accept()"


cli_serv_communic, cli_addr = server.accept()   # Aqui, o server fica bloqueado à espera que uma ligação chegue (de um connect() do cliente) para poder avançar
# O método "accept()", quando é ativado por um "connect()" do cliente, devolve um novo socket de comunicação (noutro PORT) entre o cliente que estabeleceu a ligação e o server, e ainda o endereço do cliente 
# PARA FALAR COM O CLIENTE, USAMOS O SOCKET DEVOLVIDO POR "accept()", I NÃO "server"!
# Pois, para cada "connection()" com o server, é criado um novo socket (como se fosse um túnel) para essa comunicação/ligação
# O SOCKET "server" SERVE APENAS PARA ACEITAR CONEXÕES

# Se o código está nesta linha, significa que uma conexção já foi aceite, então já podemos escrever:
print(f"Connection established with '{cli_addr}'")


# Agora queremos que o server fique pronto para aceitar novas ligações, então tem de ficar ativo até todas as ligações terminarem
while True:

    # Como, neste caso, estamos à espera que o cliente envie algo, então esperamos que ele envie: 
    cli_message = cli_serv_communic.recv(1024).decode('utf-8')     # Para avançar desta linha, é necessário que o cliente envie de facto algo (i.e., o server fica bloqueado)
    # E como quando enviamos algo, temos de codificar num byte stream a mensagem com uma determinada codificação (ex.: ascii, utf-8), então quando recebemos a mensagem temos de a descodificar

    if not cli_message:
        break

    # Pronto, agora que temos o texto do cliente, então reencaminhamo-lo:
    cli_serv_communic.sendall(("Got your message: {}".format(cli_message)).encode('utf-8'))

cli_serv_communic.close()
print(f"Communication ended with {cli_addr}.")



#if __name__ == '__main__':
#    main() # Executa a main como programa principal           
