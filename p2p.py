import socket
import threading
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

#Processa os requests de um cliente conectado
def handle_client(conn):
    while True:
        filename = conn.recv(1024).decode('utf-8') #arquivo do cliente esperado
        if not filename:
            break
        file_path = os.path.join(BASE_DIR, filename)
        if os.path.exists(file_path):
            conn.send(b"EXISTS "+str(os.path.getsize(file_path)).encode('utf-8'))
            with open(file_path,'rb') as f:
                bytes_read = f.read(1024)
                while bytes_read:
                    conn.send(bytes_read)
                    bytes_read = f.read(1024)
            print(f"Arquivo {filename} enviado com sucesso.")
        else:
            conn.send(b"ERR")
            print(f"Arquivo {filename} não encontrado.")
    conn.close()

def start_server(host = '0.0.0.0', port = 5001):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Servidor P2P iniciado em {host}:{port}")
    
    while True:
        conn, addr = server_socket.accept()
        print(f"Conexão recebida de {addr}")
        client_thread = threading.Thread(target=handle_client, args=(conn,))
        client_thread.start() 

def request_file():
    host = input("Digite o endereço IP do peer: ")
    port = int(input("Digite a porta do peer: "))
    filename = input("Digite o nome do arquivo que deseja solicitar: ")

    # Cria um socket TCP e conecta ao peer
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    client_socket.send(filename.encode('utf-8'))

    # Recebe a resposta do peer
    response = client_socket.recv(1024).decode('utf-8')
    if response.startswith("EXISTS"):
        filesize = int(response.split()[1])
        print(f"Arquivo encontrado. Tamanho: {filesize} bytes. Iniciando download...")
        with open(filename, 'wb') as f:# Salva o arquivo recebido
            bytes_received = 0
            while bytes_received < filesize:
                bytes_read = client_socket.recv(1024)
                if not bytes_read:
                    break
                f.write(bytes_read)
                bytes_received += len(bytes_read)
        print(f"Download do arquivo {filename} concluído.")
    else:
        print("Arquivo não encontrado no peer.")
    
    client_socket.close()

if __name__ == "__main__":
    choice = input("Deseja iniciar o servidor (s) ou solicitar um arquivo (r)? ")
    if choice.lower() == 's':
        start_server()
    elif choice.lower() == 'r':
        request_file()
    else:
        print("Opção inválida. Encerrando o programa.")