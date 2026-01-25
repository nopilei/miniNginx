import asyncio
import logging
from collections import deque

from config import Config
from http_utils.external.base import BaseConnection
from http_utils.external.upstream import UpstreamConnection
from metrics import POOL_LATENCY


class PoolConnectionError(Exception):
    pass


logger = logging.getLogger(__name__)


class PoolMember:
    def __init__(self, upstream_queue: asyncio.Queue, connection: BaseConnection):
        self.upstream_queue = upstream_queue
        self.connection = connection
        self.is_returned = False

    @property
    def response_is_read(self) -> bool:
        return self.connection.messages_read == 1


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

    async def acquire(self) -> PoolMember:
        upstream = self.upstreams.popleft()
        self.upstreams.append(upstream)
        loop = asyncio.get_event_loop()
        try:
            pool_start = loop.time()
            connection = await asyncio.wait_for(upstream.get(), self.connect_timeout_s)
            POOL_LATENCY.observe(loop.time() - pool_start)
        except TimeoutError as exc:
            raise PoolConnectionError("Timeout on getting upstream from pool") from exc
        return PoolMember(upstream, connection)

    async def release(self, pool_member: PoolMember, is_healthy: bool):
        if pool_member.is_returned:
            return

        queue, connection = pool_member.upstream_queue, pool_member.connection
        if is_healthy:
            await queue.put(UpstreamConnection(connection.reader, connection.writer, self.config))
        else:
            host, port = connection.writer.get_extra_info('socket').getpeername()
            await queue.put(await self.connect_upstream(host, port))
        pool_member.is_returned = True

    async def connect_upstream(self, host: str, port: int) -> BaseConnection:
        reader, writer = await asyncio.open_connection(host=host, port=port)
        return UpstreamConnection(reader, writer, self.config)
