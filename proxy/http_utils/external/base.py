import socket
import struct
from typing import Iterable

from config import Config
from http_utils.http_reader import BaseHTTPReader, HTTPMessageChunk


class BaseHTTPIterator:
    """
    Обертка над BaseHTTPReader, устанавливает таймауты на чтение данных из клиента или апстрима.
    """
    http_reader_class: type[BaseHTTPReader]
    timeout_err: Exception

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.http_reader = self.http_reader_class(sock)
        self.http_iterator = self.http_reader.chunk_iterator()
        self.messages_read = 0

    def __iter__(self):
        return self

    def __next__(self) -> HTTPMessageChunk:
        try:
            chunk = next(self.http_iterator)
            if chunk.is_message_end:
                self.messages_read += 1
            return chunk
        except TimeoutError:
            raise self.timeout_err


class BaseConnection:
    """
    Обертка над сырыми потоками asyncio.Stream*.

    Читает и пишет согласно таймаутам.
    """
    connection_closed_err: Exception
    http_iterator_class: type[BaseHTTPIterator]

    def __init__(self, sock: socket.socket, config: Config):
        self.sock = sock
        self.read_timeout_s = config.timeouts.read_ms // 1000
        self.write_timeout_s = config.timeouts.write_ms // 1000
        self.sock.settimeout(self.read_timeout_s)
        self.http_iterator = self.http_iterator_class(self.sock)

    def iterator(self) -> Iterable[HTTPMessageChunk]:
        return self.http_iterator

    @property
    def addr(self) -> tuple[str, int]:
        return self.sock.getpeername()

    @property
    def messages_read(self) -> int:
        return self.http_iterator.messages_read

    def write(self, response: bytes) -> None:
        try:
            self.sock.sendall(response)
        except Exception:
            raise self.connection_closed_err

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except Exception:
            pass
