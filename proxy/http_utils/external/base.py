import asyncio
from typing import AsyncIterable

from http_utils.http_parser import BaseHTTPReader


class BaseHTTPIterator:
    http_reader_class: type[BaseHTTPReader]
    timeout_err: Exception

    def __init__(self, reader):
        self.reader = reader
        self.http_reader = self.http_reader_class(reader)
        self.http_iterator = self.http_reader.chunk_iterator()
    
    @property
    def messages_read(self) -> int:
        return self.http_reader.messages_read

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        if self.reader.at_eof():
            raise StopAsyncIteration
        try:
            return await asyncio.wait_for(anext(self.http_iterator), 10)
        except TimeoutError as exc:
            raise self.timeout_err from exc


class BaseConnection:
    connection_closed_err: Exception
    http_iterator_class: type[BaseHTTPIterator]

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.http_iterator = self.http_iterator_class(self.reader)

    def iterator(self) -> AsyncIterable[bytes]:
        return self.http_iterator
    
    @property
    def messages_read(self) -> int:
        return self.http_iterator.messages_read

    async def write(self, response: bytes) -> None:
        if self.writer.is_closing():
            raise self.connection_closed_err
        else:
            self.writer.write(response)
            await self.writer.drain()

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

