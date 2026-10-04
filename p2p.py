import argparse
import base64
import hashlib
import json
import os
import socket
import threading
import time

CHUNK_SIZE = 700
SCAN_INTERVAL = 1.0
SYNC_INTERVAL = 4.0
HEARTBEAT_INTERVAL = 2.0
PEER_TIMEOUT = 8.0


class Peer:
    def __init__(self, host, port, peers, directory):
        self.address = (host, port)
        self.peers = peers
        self.peer_addresses = set(peers)
        self.directory = os.path.abspath(directory)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.address)
        self.lock = threading.RLock()
        self.files = {}
        self.downloads = {}
        self.last_seen = {}
        self.last_sync = 0.0
        self.last_heartbeat = 0.0
        self.request_counter = 0
        os.makedirs(self.directory, exist_ok=True)

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

    def local_files(self):
        result = {}
        for filename in os.listdir(self.directory):
            if filename.startswith('.') or filename.endswith('.part'):
                continue
            try:
                metadata = self.inspect(filename)
            except (OSError, ValueError):
                continue
            if metadata:
                result[filename] = metadata
        return result

    def send_json(self, message, destination):
        try:
            self.socket.sendto(json.dumps(message, separators=(',', ':')).encode('utf-8'), destination)
        except OSError:
            pass

    def broadcast(self, message):
        for peer in self.peers:
            self.send_json(message, peer)

    def scan(self):
        current = self.local_files()
        with self.lock:
            previous = self.files
            self.files = current
        for filename, metadata in current.items():
            if previous.get(filename) != metadata:
                self.broadcast({'type': 'ANNOUNCE', 'file': metadata})
        for filename in set(previous) - set(current):
            self.broadcast({'type': 'REMOVED', 'name': filename})

    def next_request_id(self):
        self.request_counter += 1
        return self.request_counter

    def handle_announce(self, metadata, sender):
        filename = metadata.get('name')
        if not filename or self.files.get(filename) == metadata:
            return

        request_id = self.next_request_id()
        with self.lock:
            self.downloads[request_id] = {
                'metadata': metadata,
                'filename': filename,
                'chunks': {},
                'total_chunks': (metadata['size'] + CHUNK_SIZE - 1) // CHUNK_SIZE,
                'peer': sender,
            }

        self.send_json({'type': 'GET', 'name': filename, 'request_id': request_id}, sender)

    def handle_get(self, message, sender):
        filename = message.get('name')
        request_id = message.get('request_id')
        if not filename:
            return

        try:
            metadata = self.inspect(filename)
        except (OSError, ValueError):
            return
        if metadata is None:
            return

        self.send_json({'type': 'META', 'file': metadata, 'request_id': request_id}, sender)
        if metadata['size'] == 0:
            return

        with open(self.path(filename), 'rb') as file_handle:
            seq = 0
            while chunk := file_handle.read(CHUNK_SIZE):
                self.send_json({
                    'type': 'CHUNK',
                    'name': filename,
                    'seq': seq,
                    'total_chunks': (metadata['size'] + CHUNK_SIZE - 1) // CHUNK_SIZE,
                    'data': base64.b64encode(chunk).decode('ascii'),
                    'request_id': request_id,
                }, sender)
                seq += 1

    def handle_meta(self, message):
        metadata = message.get('file')
        request_id = message.get('request_id')
        if not metadata or request_id is None:
            return

        filename = metadata.get('name')
        if not filename:
            return
        if self.files.get(filename) == metadata:
            return

        if metadata.get('size') == 0:
            path = self.path(filename)
            try:
                with open(path, 'wb'):
                    pass
                with self.lock:
                    self.files[filename] = metadata
                    self.downloads.pop(request_id, None)
                print(f'[{self.address[1]}] arquivo recebido: {filename}')
            except (OSError, ValueError):
                pass
            return

        with self.lock:
            self.downloads[request_id] = {
                'metadata': metadata,
                'filename': filename,
                'chunks': {},
                'total_chunks': (metadata['size'] + CHUNK_SIZE - 1) // CHUNK_SIZE,
            }

    def handle_chunk(self, message):
        request_id = message.get('request_id')
        if request_id is None:
            return

        with self.lock:
            download = self.downloads.get(request_id)
            if download is None:
                return
            try:
                seq = int(message['seq'])
            except (KeyError, TypeError, ValueError):
                return
            raw_data = message.get('data', '')
            if raw_data:
                download['chunks'][seq] = base64.b64decode(raw_data)
            else:
                download['chunks'][seq] = b''

            if len(download['chunks']) < download['total_chunks']:
                return

            metadata = download['metadata']
            filename = download['filename']
            chunks = dict(download['chunks'])
            self.downloads.pop(request_id, None)

        try:
            path = self.path(filename)
            temporary = path + '.part'
            assembled = bytearray()
            for index in range(download['total_chunks']):
                assembled.extend(chunks.get(index, b''))
            with open(temporary, 'wb') as file_handle:
                file_handle.write(assembled)

            if hashlib.sha256(assembled).hexdigest() != metadata.get('sha256'):
                if os.path.exists(temporary):
                    os.remove(temporary)
                print(f'[{self.address[1]}] checksum inválido para {filename}')
                return

            os.replace(temporary, path)
            with self.lock:
                self.files[filename] = metadata
            print(f'[{self.address[1]}] arquivo recebido: {filename}')
        except (OSError, ValueError):
            pass

    def process_message(self, message, sender):
        message_type = message.get('type')

        if message_type == 'LIST_REQ':
            with self.lock:
                payload = list(self.files.values())
            self.send_json({'type': 'LIST_RESP', 'files': payload}, sender)
            return

        if message_type == 'LIST_RESP':
            for item in message.get('files', []):
                if isinstance(item, dict) and item.get('name') and self.files.get(item['name']) != item:
                    self.handle_announce(item, sender)
            return

        if message_type == 'ANNOUNCE':
            metadata = message.get('file', {})
            if isinstance(metadata, dict):
                self.handle_announce(metadata, sender)
            return

        if message_type == 'GET':
            self.handle_get(message, sender)
            return

        if message_type == 'META':
            self.handle_meta(message)
            return

        if message_type == 'CHUNK':
            self.handle_chunk(message)
            return

        if message_type == 'REMOVED':
            filename = message.get('name')
            if not filename:
                return
            try:
                path = self.path(filename)
            except ValueError:
                return
            if os.path.exists(path):
                os.remove(path)
                with self.lock:
                    self.files.pop(filename, None)
                print(f'[{self.address[1]}] arquivo removido: {filename}')
            return

        if message_type == 'HEARTBEAT':
            with self.lock:
                self.last_seen[sender] = time.monotonic()
            return

    def listener(self):
        while True:
            try:
                data, sender = self.socket.recvfrom(65535)
                if sender in self.peer_addresses:
                    with self.lock:
                        self.last_seen[sender] = time.monotonic()
                message = json.loads(data.decode('utf-8'))
                self.process_message(message, sender)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError):
                continue

    def status(self):
        now = time.monotonic()
        with self.lock:
            files = sorted(self.files)
            active = sorted(
                f'{host}:{port}'
                for (host, port), timestamp in self.last_seen.items()
                if now - timestamp <= PEER_TIMEOUT
            )
        print(f'[{self.address[1]}] peers ativos: {active} ({len(active)}/{len(self.peers)} configurados) | arquivos: {len(files)} | {files}')

    def run(self):
        threading.Thread(target=self.listener, daemon=True).start()
        self.scan()
        self.broadcast({'type': 'LIST_REQ'})
        print(f'Peer UDP iniciado em {self.address[0]}:{self.address[1]} | tmp={self.directory}')

        while True:
            time.sleep(SCAN_INTERVAL)
            self.scan()
            if time.monotonic() - self.last_heartbeat >= HEARTBEAT_INTERVAL:
                self.broadcast({'type': 'HEARTBEAT'})
                self.last_heartbeat = time.monotonic()
            if time.monotonic() - self.last_sync >= SYNC_INTERVAL:
                self.broadcast({'type': 'LIST_REQ'})
                self.last_sync = time.monotonic()
                self.status()


def parse_peer(value):
    host, separator, port = value.rpartition(':')
    if not separator or not host:
        raise argparse.ArgumentTypeError('peer deve usar HOST:PORTA')
    try:
        return host, int(port)
    except ValueError as error:
        raise argparse.ArgumentTypeError('porta inválida') from error


def main():
    parser = argparse.ArgumentParser(description='Peer P2P de sincronização de arquivos via UDP')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--peer', action='append', type=parse_peer, default=[])
    parser.add_argument('--dir', default=None, help='diretório tmp deste peer')
    args = parser.parse_args()

    directory = args.dir or os.path.join(os.path.dirname(os.path.abspath(__file__)), f'tmp_{args.port}')
    peers = [peer for peer in args.peer if peer != (args.host, args.port) and peer != ('0.0.0.0', args.port)]
    Peer(args.host, args.port, peers, directory).run()


if __name__ == '__main__':
    main()
