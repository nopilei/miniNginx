import http
import logging
import socket
from dataclasses import dataclass
from typing import Generator


class HTTPParseError(Exception):
    pass


@dataclass
class HTTPMessageChunk:
    chunk: bytes
    is_message_start: bool
    is_message_end: bool


logger = logging.getLogger(__name__)


class BaseHTTPReader:
    """
    Читает из asyncio.StreamReader байты по чанкам, парсит и валидирует по формату HTTP сообщения.

    Таймауты обрабатываются в клиентском коде.
    """
    MIN_VERSION = b'HTTP/1.1'

    def __init__(self, sock: socket.socket):
        self.sock_file = sock.makefile('rb')

    def chunk_iterator(self) -> Generator[HTTPMessageChunk, None, None]:
        try:
            for chunk in self._chunk_iterator():
                if not chunk.chunk:
                    raise StopIteration
                yield chunk
        except ValueError:
            raise HTTPParseError('Invalid bytes from external resource')

    def _chunk_iterator(self) -> Generator[HTTPMessageChunk, None, None]:
        while True:
            start_line = self._get_start_line()
            self._validate_start_line(start_line)
            headers = self._get_headers()
            yield HTTPMessageChunk(start_line + headers, is_message_start=True, is_message_end=False)
            for chunk in self._get_body(headers):
                yield chunk

    def _get_headers(self) -> bytes:
        headers = b''

        while True:
            line = self.sock_file.readline()
            headers += line
            if line == b'\r\n':
                break
        return headers

    def _get_body(self, headers: bytes) -> Generator[HTTPMessageChunk, None, None]:
        lower_case_headers = self._get_parsed_headers(headers)

        if content_length := int(lower_case_headers.get(b'content-length', 0)):
            chunk_size = 512
            bytes_read = 0
            while bytes_read < content_length:
                to_read = min(chunk_size, content_length - bytes_read)
                is_message_end = content_length - bytes_read <= chunk_size
                chunk = self.sock_file.read(to_read)
                yield HTTPMessageChunk(chunk, is_message_start=False, is_message_end=is_message_end)
                bytes_read += to_read
        else:
            yield HTTPMessageChunk(b'', is_message_start=False, is_message_end=True)

    def _get_parsed_headers(self, raw_headers: bytes) -> dict[bytes, bytes]:
        headers = {}

        for header_line in raw_headers[:-4].split(b'\r\n'):
            name, value = header_line.split(b':', 1)
            headers[name.lower()] = value.strip()

        return headers

    def _get_start_line(self) -> bytes:
        return self.sock_file.readline()

    def _validate_start_line(self, raw_start_line: bytes) -> None:
        raise NotImplementedError


class HTTPRequestReader(BaseHTTPReader):
    def _validate_start_line(self, raw_start_line: bytes) -> None:
        method, path, version = raw_start_line[:-2].split(b' ')
        # logger.info(f"Getting request. {method} {path} {version}")

        if http.HTTPMethod(method.decode()) not in http.HTTPMethod:
            raise ValueError(f'Wrong method: {method}')

        if not path:
            raise ValueError(f'Empty path')

        if not version.startswith(b'HTTP/') and version < self.MIN_VERSION:
            raise ValueError(f'Invalid version: {version}')


class HTTPResponseReader(BaseHTTPReader):
    def _validate_start_line(self, raw_start_line: bytes) -> None:
        version, status, reason = raw_start_line[:-2].split(b' ', 2)
        # logger.info(f"Getting response. {status} {reason}")

        if http.HTTPStatus(int(status)) not in http.HTTPStatus:
            raise ValueError(f'Wrong status code: {status}')

        if not version.startswith(b'HTTP/') and version < self.MIN_VERSION:
            raise ValueError(f'Invalid version: {version}')
