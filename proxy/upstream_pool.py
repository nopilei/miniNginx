import asyncio
from collections import deque
from typing import Coroutine

from config import ParsedConfig
from http_utils.external.upstream import UpstreamConnection


class PoolConnectionError(Exception):
    pass


class UpstreamPool:
    def __init__(self, config: ParsedConfig):
        self.config = config

    async def prepare_connections(self):
        raise NotImplementedError

    async def acquire(self):
        raise NotImplementedError


class RoundRobinUpstreamPool(UpstreamPool):
    def __init__(self, config: ParsedConfig):
        super().__init__(config)
        self.upstreams: deque[asyncio.Queue[UpstreamConnection]] = deque()

    async def prepare_connections(self) -> None:
        for upstream in self.config.upstreams:
            host, port = upstream["host"], int(upstream["port"])
            connections = asyncio.Queue()
            for _ in range(1):
                try:
                    connection = await self.connect_upstream(host, port)
                    await connections.put(connection)
                except Exception as exc:
                    print(exc)
            if connections:
                self.upstreams.append(connections)
        if not self.upstreams:
            raise PoolConnectionError("Failed connect to upstreams")

    async def acquire(self) -> tuple[UpstreamConnection, asyncio.Queue]:
        upstream = self.upstreams.popleft()
        self.upstreams.append(upstream)

        try:
            connection = await asyncio.wait_for(upstream.get(), 10)
        except TimeoutError as exc:
            raise PoolConnectionError("Timeout on getting upstream from pool") from exc
        return connection, upstream

    async def release(self, upstream: asyncio.Queue, connection: UpstreamConnection):
        await upstream.put(connection)

    async def connect_upstream(self, host: str, port: int) -> UpstreamConnection:
        reader, writer = await asyncio.open_connection(host=host, port=port)
        return UpstreamConnection(reader, writer)
