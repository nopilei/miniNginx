import asyncio
import http
import logging
from dataclasses import dataclass
from typing import AsyncGenerator


class HTTPParseError(Exception):
    pass


@dataclass
class HTTPMessageChunk:
    chunk: bytes
    is_message_start: bool
    is_message_end: bool


logger = logging.getLogger(__name__)


@dataclass
class HTTPResponse:
    version: bytes
    status: bytes
    reason: bytes
    headers: dict[bytes, bytes]
    body: bytes

    def add_header(self, key: bytes, value: bytes) -> None:
        self.headers[key] = value

    @property
    def full(self) -> bytes:
        crlf = b'\r\n'
        request_line = b' '.join([self.version, self.status, self.reason])
        headers = crlf.join(k + b': ' + v for k, v in self.headers.items())
        body = self.body
        return request_line + crlf + headers + crlf * 2 + body


class BaseHTTPReader:
    MIN_VERSION = b'HTTP/1.1'

    def __init__(self, reader: asyncio.StreamReader):
        self.reader = reader

    async def chunk_iterator(self) -> AsyncGenerator[HTTPMessageChunk, None]:
        try:
            async for chunk in self._chunk_iterator():
                yield chunk
        except Exception as exc:
            raise HTTPParseError('Invalid bytes from external resource') from exc

    async def _chunk_iterator(self) -> AsyncGenerator[HTTPMessageChunk, None]:
        while True:
            start_line = await self._get_start_line()
            self._validate_start_line(start_line)
            headers = await self._get_headers()
            yield HTTPMessageChunk(start_line + headers, is_message_start=True, is_message_end=False)
            async for chunk in self._get_body(headers):
                yield chunk

    async def _get_headers(self) -> bytes:
        return await self.reader.readuntil(b'\r\n\r\n')

    async def _get_body(self, headers: bytes) -> AsyncGenerator[HTTPMessageChunk, None]:
        lower_case_headers = self._get_parsed_headers(headers)

        if content_length := int(lower_case_headers.get(b'content-length', 0)):
            chunk_size = 512
            bytes_read = 0
            while bytes_read < content_length:
                to_read = min(chunk_size, content_length - bytes_read)
                is_message_end = content_length - bytes_read <= chunk_size
                chunk = await self.reader.readexactly(to_read)
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

    async def _get_start_line(self) -> bytes:
        return await self.reader.readuntil(b'\r\n')

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
