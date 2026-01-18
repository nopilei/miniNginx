import asyncio
import logging
from collections import deque

from config import Config
from http_utils.external.base import BaseConnection
from http_utils.external.upstream import UpstreamConnection


class PoolConnectionError(Exception):
    pass


logger = logging.getLogger(__name__)


class RoundRobinUpstreamPool:
    def __init__(self, config: Config):
        self.config = config
        self.upstream_addrs = config.upstreams
        self.connect_timeout_s = config.timeouts.connect_ms / 1000
        self.upstreams: deque[asyncio.Queue[BaseConnection]] = deque()

    async def prepare_connections(self) -> None:
        for upstream_addr in self.upstream_addrs:
            host, port = upstream_addr.host, upstream_addr.port
            connections = asyncio.Queue()
            for _ in range(self.config.limits.max_conns_per_upstream):
                try:
                    connection = await self.connect_upstream(host, port)
                    await connections.put(connection)
                except Exception as exc:
                    logger.exception(exc)
            if connections:
                self.upstreams.append(connections)
        if not self.upstreams:
            raise PoolConnectionError("Failed connect to upstreams")

    async def acquire(self) -> tuple[BaseConnection, asyncio.Queue]:
        upstream = self.upstreams.popleft()
        self.upstreams.append(upstream)

        try:
            connection = await asyncio.wait_for(upstream.get(), self.connect_timeout_s)
        except TimeoutError as exc:
            raise PoolConnectionError("Timeout on getting upstream from pool") from exc
        return connection, upstream

    async def release(self, upstream: asyncio.Queue, connection: BaseConnection, is_healthy: bool):
        if is_healthy:
            await upstream.put(UpstreamConnection(connection.reader, connection.writer, self.config))
        else:
            host, port = connection.writer.get_extra_info('socket').getpeername()
            await upstream.put(await self.connect_upstream(host, port))

    async def connect_upstream(self, host: str, port: int) -> BaseConnection:
        reader, writer = await asyncio.open_connection(host=host, port=port)
        return UpstreamConnection(reader, writer, self.config)
