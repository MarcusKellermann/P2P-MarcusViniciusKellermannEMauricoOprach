import argparse
import base64
import hashlib
import json
import os
import socket
import threading
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

CHUNK_SIZE = 700


def send_tcp(sock, message):
    data = (json.dumps(message, separators=(',', ':')) + '\n').encode('utf-8')
    sock.sendall(data)


class Client:
    def __init__(self, host, port, directory):
        self.directory = os.path.abspath(directory)
        os.makedirs(self.directory, exist_ok=True)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        print('Conectado!\n')

        self.lock = threading.Lock()
        self.ignore_created = set()
        self.ignore_deleted = set()
        self.downloads = {}
        self.last_metadata = {}

        self.observer = Observer()
        self.observer.schedule(Handler(self), path=self.directory, recursive=False)
        self.observer.start()

        self.announce_existing_files()
        threading.Thread(target=self.tcp_message, daemon=True).start()

    def path(self, filename):
        safe_name = os.path.basename(filename)
        if safe_name != filename or not safe_name or safe_name.startswith('.'):
            raise ValueError('nome de arquivo inválido')
        return os.path.join(self.directory, safe_name)

    def inspect(self, filename):
        file_path = self.path(filename)
        if not os.path.isfile(file_path):
            return None
        digest = hashlib.sha256()
        size = 0
        with open(file_path, 'rb') as file_handle:
            while chunk := file_handle.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
        return {'name': filename, 'size': size, 'sha256': digest.hexdigest()}

    def announce_existing_files(self):
        for filename in os.listdir(self.directory):
            if filename.startswith('.') or filename.endswith('.part'):
                continue
            try:
                metadata = self.inspect(filename)
            except (OSError, ValueError):
                continue
            if metadata:
                self.last_metadata[filename] = metadata
                self.send({'type': 'ANNOUNCE', 'file': metadata})

    def send(self, message):
        try:
            with self.lock:
                send_tcp(self.socket, message)
        except OSError as error:
            print(f'Erro ao enviar para o servidor: {error}')

    def tcp_message(self):
        buffer = b''
        while True:
            try:
                data = self.socket.recv(65535)
                if not data:
                    print('\nConexão encerrada pelo servidor.')
                    break
                buffer += data

                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    if not line:
                        continue
                    message = json.loads(line.decode('utf-8'))
                    self.process_server_message(message)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError) as error:
                print(f'\nConexão perdida: {error}')
                break

    def process_server_message(self, message):
        message_type = message.get('type')

        if message_type == 'GET':
            self.send_file(message.get('name', ''), message.get('request_id', ''))

        elif message_type == 'ANNOUNCE':
            self.receive_announce(message)

        elif message_type == 'META':
            self.receive_meta(message)

        elif message_type == 'CHUNK':
            self.receive_chunk(message)

        elif message_type == 'REMOVED':
            self.remove_file_from_network(message.get('name', ''))

        elif message_type == 'LIST_REQ':
            self.send_list()

    def receive_announce(self, message):
        metadata = message.get('file', {})
        peer = message.get('_peer')
        filename = metadata.get('name') if isinstance(metadata, dict) else None
        if not filename or not isinstance(peer, list) or len(peer) != 2:
            return

        try:
            local = self.inspect(filename)
        except (OSError, ValueError):
            local = None

        # Para evitar que o cliente peça arquivo já existente
        if local == metadata:
            self.last_metadata[filename] = local
            return

        self.send({
            'type': 'GET',
            'name': filename,
            'sha256': metadata.get('sha256', ''),
            '_peer': [peer[0], int(peer[1])],
        })

    def send_list(self):
        for filename in os.listdir(self.directory):
            if filename.startswith('.') or filename.endswith('.part'):
                continue
            try:
                metadata = self.inspect(filename)
            except (OSError, ValueError):
                continue
            if metadata:
                self.send({'type': 'ANNOUNCE', 'file': metadata})

    def send_file(self, filename, request_id):
        try:
            metadata = self.inspect(filename)
            if not metadata:
                return

            self.send({
                'type': 'META',
                'file': metadata,
                'request_id': request_id,
            })

            with open(self.path(filename), 'rb') as file_handle:
                sequence = 0
                while chunk := file_handle.read(CHUNK_SIZE):
                    self.send({
                        'type': 'CHUNK',
                        'name': filename,
                        'seq': sequence,
                        'total': metadata['size'],
                        'data': base64.b64encode(chunk).decode('ascii'),
                        'request_id': request_id,
                    })
                    sequence += 1

            # Arquivo vazio não possui CHUNK.
            if metadata['size'] == 0:
                self.send({
                    'type': 'CHUNK',
                    'name': filename,
                    'seq': 0,
                    'total': 0,
                    'data': '',
                    'request_id': request_id,
                })
        except (OSError, ValueError):
            pass

    def receive_meta(self, message):
        metadata = message.get('file', {})
        filename = metadata.get('name')
        request_id = message.get('request_id')
        if not filename or not request_id:
            return

        try:
            path = self.path(filename)
        except ValueError:
            return

        with self.lock:
            self.downloads[request_id] = {
                'metadata': metadata,
                'chunks': {},
                'total_chunks': (metadata['size'] + CHUNK_SIZE - 1) // CHUNK_SIZE,
                'filename': filename,
            }

        if metadata.get('size') == 0:
            try:
                with self.lock:
                    self.ignore_created.add(filename)
                with open(path, 'wb'):
                    pass
                self.last_metadata[filename] = metadata
                with self.lock:
                    self.downloads.pop(request_id, None)
                print(f'Arquivo recebido pela rede: {filename}')
            except OSError:
                pass

    def receive_chunk(self, message):
        request_id = message.get('request_id')
        if not request_id:
            return

        with self.lock:
            download = self.downloads.get(request_id)
            if not download:
                return
            try:
                sequence = int(message['seq'])
                download['chunks'][sequence] = message.get('data', '')
            except (KeyError, ValueError, TypeError):
                return

            complete = len(download['chunks']) >= download['total_chunks']
            if not complete:
                return
            metadata = download['metadata']
            chunks = dict(download['chunks'])
            filename = download['filename']
            self.downloads.pop(request_id, None)

        try:
            path = self.path(filename)
            temporary = path + '.part'
            with open(temporary, 'wb') as file_handle:
                for index in range(download['total_chunks']):
                    file_handle.write(base64.b64decode(chunks[index]))

            digest = self.inspect_temporary(temporary)
            if digest != metadata.get('sha256'):
                os.remove(temporary)
                print(f'Erro: checksum inválido para {filename}')
                return

            with self.lock:
                self.ignore_created.add(filename)
            os.replace(temporary, path)
            self.last_metadata[filename] = metadata
            print(f'Arquivo recebido pela rede: {filename}')
        except (KeyError, OSError, ValueError, base64.binascii.Error):
            try:
                if os.path.exists(temporary):
                    os.remove(temporary)
            except OSError:
                pass

    def inspect_temporary(self, path):
        digest = hashlib.sha256()
        with open(path, 'rb') as file_handle:
            while chunk := file_handle.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()

    def remove_file_from_network(self, filename):
        if not filename:
            return
        try:
            path = self.path(filename)
        except ValueError:
            return

        with self.lock:
            self.ignore_deleted.add(filename)

        try:
            if os.path.exists(path):
                os.remove(path)
                self.last_metadata.pop(filename, None)
                print(f'Arquivo removido pela rede: {filename}')
            else:
                with self.lock:
                    self.ignore_deleted.discard(filename)
        except OSError:
            with self.lock:
                self.ignore_deleted.discard(filename)

    def handle_created(self, filename):
        with self.lock:
            if filename in self.ignore_created:
                self.ignore_created.remove(filename)
                return

        try:
            metadata = self.inspect(filename)
        except (OSError, ValueError):
            return
        if not metadata:
            return

        previous = self.last_metadata.get(filename)
        self.last_metadata[filename] = metadata
        if previous != metadata:
            print(f'Arquivo criado: {filename}')
            self.send({'type': 'ANNOUNCE', 'file': metadata})

    def handle_deleted(self, filename):
        with self.lock:
            if filename in self.ignore_deleted:
                self.ignore_deleted.remove(filename)
                return

        self.last_metadata.pop(filename, None)
        print(f'Arquivo removido: {filename}')
        self.send({'type': 'REMOVED', 'name': filename})

    def run(self):
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()


class Handler(FileSystemEventHandler):
    def __init__(self, client):
        self.client = client

    def on_created(self, event):
        if not event.is_directory:
            filename = os.path.basename(event.src_path)
            if not filename.endswith('.part'):
                time.sleep(0.05)
                self.client.handle_created(filename)

    def on_modified(self, event):
        if not event.is_directory:
            filename = os.path.basename(event.src_path)
            if not filename.endswith('.part'):
                time.sleep(0.05)
                self.client.handle_created(filename)

    def on_deleted(self, event):
        if not event.is_directory:
            filename = os.path.basename(event.src_path)
            if not filename.endswith('.part'):
                self.client.handle_deleted(filename)


def main():
    parser = argparse.ArgumentParser(description='Client TCP do peer P2P')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, required=True, help='porta TCP local do p2p.py')
    parser.add_argument('--dir', default=None, help='diretório compartilhado deste peer')
    args = parser.parse_args()

    directory = args.dir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        f'tmp_{args.port}'
    )

    client = Client(args.host, args.port, directory)
    client.run()


if __name__ == '__main__':
    main()
