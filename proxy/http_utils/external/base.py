import asyncio
from typing import AsyncIterable

from config import Config
from http_utils.http_reader import BaseHTTPReader


class BaseHTTPIterator:
    http_reader_class: type[BaseHTTPReader]
    timeout_err: Exception

    def __init__(self, reader: asyncio.StreamReader, read_timeout_s: float):
        self.reader = reader
        self.read_timeout = read_timeout_s
        self.http_reader = self.http_reader_class(reader)
        self.http_iterator = self.http_reader.chunk_iterator()
    
    @property
    def messages_read(self) -> int:
        return self.http_reader.messages_read

    @property
    def messages_read_timestamps(self) -> asyncio.Queue:
        return self.http_reader.messages_read_timestamps

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

    def __init__(
            self,
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
            config: Config,
    ):
        self.reader = reader
        self.writer = writer
        self.read_timeout_s = config.timeouts.read_ms / 1000
        self.write_timeout_s = config.timeouts.write_ms / 1000
        self.http_iterator = self.http_iterator_class(self.reader, self.read_timeout_s)

    def iterator(self) -> AsyncIterable[bytes]:
        return self.http_iterator

    @property
    def addr(self) -> tuple[str, int]:
        return self.writer.get_extra_info("socket").getpeername()
    
    @property
    def messages_read(self) -> int:
        return self.http_iterator.messages_read

    @property
    def messages_read_timestamps(self) -> asyncio.Queue:
        return self.http_iterator.messages_read_timestamps

    async def write(self, response: bytes) -> None:
        if self.writer.is_closing():
            raise self.connection_closed_err
        else:
            self.writer.write(response)
            await asyncio.wait_for(self.writer.drain(), self.write_timeout_s)

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

