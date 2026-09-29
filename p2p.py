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
SYNC_INTERVAL = 3.0
HEARTBEAT_INTERVAL = 2.0
PEER_TIMEOUT = 8.0


class Peer:
    def __init__(self, host, port, peers, directory):
        self.address = (host, port)
        self.peers = peers
        self.peer_addresses = set()
        for peer_host, peer_port in peers:
            try:
                self.peer_addresses.add((socket.gethostbyname(peer_host), peer_port))
            except OSError:
                continue
        self.directory = os.path.abspath(directory)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.address)
        self.lock = threading.RLock()
        self.files = {}
        self.downloads = {}
        self.last_seen = {}
        self.last_sync = 0.0
        self.last_heartbeat = 0.0
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

    def send(self, message, destination):
        try:
            data = json.dumps(message, separators=(',', ':')).encode('utf-8')
            self.socket.sendto(data, destination)
        except OSError:
            pass

    def broadcast(self, message):
        for peer in self.peers:
            self.send(message, peer)

    def announce(self, metadata):
        self.broadcast({'type': 'ANNOUNCE', 'file': metadata})

    def scan(self):
        current = self.local_files()
        with self.lock:
            previous = self.files
            self.files = current
        for filename, metadata in current.items():
            if previous.get(filename) != metadata:
                self.announce(metadata)
        for filename in set(previous) - set(current):
            self.broadcast({'type': 'REMOVED', 'name': filename})

    def request_file(self, filename, metadata, peer):
        with self.lock:
            download = self.downloads.get(filename)
            if download and download['metadata'] == metadata:
                return
            self.downloads[filename] = {
                'metadata': metadata,
                'chunks': {},
                'total': (metadata['size'] + CHUNK_SIZE - 1) // CHUNK_SIZE,
                'peer': peer,
                'last_request': 0.0,
            }
        self.send({'type': 'GET', 'name': filename, 'sha256': metadata['sha256']}, peer)

    def send_file(self, filename, destination):
        try:
            metadata = self.inspect(filename)
            if not metadata:
                return
            self.send({'type': 'META', 'file': metadata}, destination)
            with open(self.path(filename), 'rb') as file_handle:
                sequence = 0
                while chunk := file_handle.read(CHUNK_SIZE):
                    self.send({
                        'type': 'CHUNK',
                        'name': filename,
                        'seq': sequence,
                        'total': metadata['size'],
                        'data': base64.b64encode(chunk).decode('ascii'),
                    }, destination)
                    sequence += 1
        except (OSError, ValueError):
            return

    def receive_meta(self, metadata, sender):
        filename = metadata.get('name')
        if not filename:
            return
        with self.lock:
            existing = self.files.get(filename)
        if existing == metadata:
            return
        if metadata.get('size') == 0:
            if metadata.get('sha256') != hashlib.sha256(b'').hexdigest():
                return
            try:
                temporary = self.path(filename) + '.part'
                with open(temporary, 'wb'):
                    pass
                os.replace(temporary, self.path(filename))
                with self.lock:
                    self.downloads.pop(filename, None)
                self.scan()
                print(f'[{self.address[1]}] arquivo recebido: {filename}')
            except (OSError, ValueError):
                pass
            return
        self.request_file(filename, metadata, sender)

    def receive_chunk(self, message):
        filename = message.get('name')
        sequence = message.get('seq')
        if not isinstance(filename, str) or not isinstance(sequence, int):
            return
        with self.lock:
            download = self.downloads.get(filename)
            if not download:
                return
            download['chunks'][sequence] = message.get('data', '')
            complete = len(download['chunks']) >= download['total']
            if complete:
                chunks = download['chunks']
                metadata = download['metadata']
        if complete:
            try:
                temporary = self.path(filename) + '.part'
                with open(temporary, 'wb') as file_handle:
                    for index in range(download['total']):
                        file_handle.write(base64.b64decode(chunks[index]))
                if self.inspect(filename) != metadata:
                    os.replace(temporary, self.path(filename))
                else:
                    os.remove(temporary)
                with self.lock:
                    self.downloads.pop(filename, None)
                self.scan()
                print(f'[{self.address[1]}] arquivo recebido: {filename}')
            except (KeyError, OSError, ValueError):
                pass

    def process(self, message, sender):
        message_type = message.get('type')
        if message_type == 'LIST_REQ':
            with self.lock:
                files = list(self.files.values())
            self.send({'type': 'LIST_RESP', 'files': files}, sender)
        elif message_type == 'LIST_RESP':
            for metadata in message.get('files', []):
                if isinstance(metadata, dict) and metadata.get('name'):
                    self.receive_meta(metadata, sender)
        elif message_type == 'ANNOUNCE':
            metadata = message.get('file', {})
            if isinstance(metadata, dict):
                self.receive_meta(metadata, sender)
        elif message_type == 'GET':
            self.send_file(message.get('name', ''), sender)
        elif message_type == 'META':
            self.receive_meta(message.get('file', {}), sender)
        elif message_type == 'CHUNK':
            self.receive_chunk(message)
        elif message_type == 'REMOVED':
            try:
                file_path = self.path(message.get('name', ''))
                if os.path.exists(file_path):
                    os.remove(file_path)
                    self.scan()
                    print(f'[{self.address[1]}] arquivo removido: {message["name"]}')
            except (OSError, ValueError, KeyError):
                pass

    def listener(self):
        while True:
            try:
                data, sender = self.socket.recvfrom(65535)
                if sender in self.peer_addresses:
                    with self.lock:
                        self.last_seen[sender] = time.monotonic()
                message = json.loads(data.decode('utf-8'))
                self.process(message, sender)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError):
                continue

    def retry_downloads(self):
        now = time.monotonic()
        with self.lock:
            pending = list(self.downloads.items())
        for filename, download in pending:
            if now - download['last_request'] < 1.5:
                continue
            download['last_request'] = now
            self.send({'type': 'GET', 'name': filename, 'sha256': download['metadata']['sha256']}, download['peer'])

    def status(self):
        now = time.monotonic()
        with self.lock:
            files = sorted(self.files)
            pending = len(self.downloads)
            active_peers = sorted(
                f'{host}:{port}'
                for (host, port), last_seen in self.last_seen.items()
                if now - last_seen <= PEER_TIMEOUT
            )
        print(f'[{self.address[1]}] peers ativos: {active_peers} ({len(active_peers)}/{len(self.peers)} configurados) | arquivos: {len(files)} | pendentes: {pending} | {files}')

    def run(self):
        threading.Thread(target=self.listener, daemon=True).start()
        self.scan()
        self.broadcast({'type': 'LIST_REQ'})
        print(f'Peer UDP iniciado em {self.address[0]}:{self.address[1]} | tmp={self.directory}')
        while True:
            time.sleep(SCAN_INTERVAL)
            self.scan()
            self.retry_downloads()
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