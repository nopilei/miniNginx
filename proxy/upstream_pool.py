import logging
import queue
import socket
import time
from collections import deque

from config import Config
from http_utils.external.base import BaseConnection
from http_utils.external.upstream import UpstreamConnection
from metrics import POOL_LATENCY


class PoolConnectionError(Exception):
    pass


logger = logging.getLogger(__name__)


class PoolMember:
    def __init__(self, upstream_queue: queue.Queue, connection: BaseConnection):
        self.upstream_queue = upstream_queue
        self.connection = connection
        self.is_returned = False

    @property
    def response_is_read(self) -> bool:
        return self.connection.messages_read == 1


class RoundRobinUpstreamPool:
    """
    Менеджер пула соединений к апстримам.

    Выдача соединения идет по round-robin.
    """
    def __init__(self, config: Config):
        self.config = config
        self.upstream_addrs = config.upstreams
        self.connect_timeout_s = config.timeouts.connect_ms / 1000
        self.upstreams: deque[queue.Queue[BaseConnection]] = deque()

    def prepare_connections(self) -> None:
        for upstream_addr in self.upstream_addrs:
            host, port = upstream_addr.host, upstream_addr.port
            connections = queue.Queue()
            for i in range(self.config.limits.max_conns_per_upstream):
                try:
                    connection = self.connect_upstream(host, port)
                    connections.put(connection)
                except Exception as exc:
                    logger.exception(exc)
            if connections:
                self.upstreams.append(connections)
        if not self.upstreams:
            raise PoolConnectionError("Failed connect to upstreams")

    def acquire(self) -> PoolMember:
        upstream = self.upstreams.popleft()
        self.upstreams.append(upstream)

        try:
            pool_start = time.monotonic()
            connection = upstream.get(timeout=self.connect_timeout_s)
            POOL_LATENCY.observe(time.monotonic() - pool_start)
        except queue.Empty:
            raise PoolConnectionError("Timeout on getting upstream from pool")
        return PoolMember(upstream, connection)

    def release(self, pool_member: PoolMember, is_healthy: bool):
        if pool_member.is_returned:
            return

        upstream_queue, connection = pool_member.upstream_queue, pool_member.connection
        if is_healthy:
            upstream_queue.put(UpstreamConnection(connection.sock, self.config))
        else:
            host, port = connection.sock.getpeername()
            upstream_queue.put(self.connect_upstream(host, port))
        pool_member.is_returned = True

    def connect_upstream(self, host: str, port: int) -> BaseConnection:
        sock = socket.create_connection((host, port), timeout=self.connect_timeout_s)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        return UpstreamConnection(sock, self.config)
