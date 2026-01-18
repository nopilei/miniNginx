import asyncio
import logging

from config import Config
from http_utils.error_responses import get_error_response
from http_utils.external.base import BaseConnection
from http_utils.http_reader import HTTPParseError
from http_utils.external.upstream import UpstreamConnectionTimeout
from metrics import REQUEST_LATENCY, UPSTREAM_TIMEOUTS
from upstream_pool import PoolConnectionError, RoundRobinUpstreamPool
from http_utils.external.client import ClientConnectionTimeout, ClientConnectionClosed, ClientConnection
from context import client_addr_var

logger = logging.getLogger(__name__)


class ProxyServer:
    def __init__(self, config: Config):
        self.config = config
        self.conn_semaphore = asyncio.Semaphore(config.limits.max_client_conns)
        self.total_timeout_s = self.config.timeouts.total_ms / 1000
        self.pool = RoundRobinUpstreamPool(config)

    async def client_handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async with self.conn_semaphore:
            client_connection = ClientConnection(reader, writer, self.config)
            token = client_addr_var.set(str(client_connection.addr))
            logger.info("Got new client connection.")
            try:
                await asyncio.wait_for(self.process_client_connection(client_connection), self.total_timeout_s)
            except TimeoutError:
                logger.info("Keep alive connection with client closed forcefully: too long session!")
                await client_connection.close()
            client_addr_var.reset(token)

    async def process_client_connection(self, client_conn: BaseConnection) -> None:
        logger.info('Processing new client connection.')

        try:
            upstream_connection, upstream_queue = await self.pool.acquire()
        except PoolConnectionError as exc:
            logger.error(exc)
            await self.send_bad_gateway_response(client_conn)
            await client_conn.close()
            return
        logger.info(f"Got upstream connection: {upstream_connection.addr}")

        try:
            await self.proxy_client_connection(client_conn, upstream_connection)
        except asyncio.CancelledError:
            logger.info("Proxing client cancelled.")
            await self.cleanup(client_conn, upstream_connection, upstream_queue)
            raise
        else:
            await self.cleanup(client_conn, upstream_connection, upstream_queue)

    async def cleanup(
            self,
            client_conn: BaseConnection,
            upstream_conn: BaseConnection,
            upstream_queue: asyncio.Queue,
    ) -> None:
        await client_conn.close()
        is_healthy = client_conn.messages_read == upstream_conn.messages_read
        await self.pool.release(upstream_queue, upstream_conn, is_healthy)

    async def proxy_client_connection(self, client_conn: BaseConnection, upstream_conn: BaseConnection) -> None:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.client_to_upstream(client_conn, upstream_conn))
                tg.create_task(self.upstream_to_client(client_conn, upstream_conn))
                tg.create_task(self.measure_latency(client_conn, upstream_conn))
        except* (ClientConnectionTimeout, ClientConnectionClosed):
            logger.info("Client timeout.")
        except* HTTPParseError:
            logger.error("Error parsing http data")
            await self.send_parsing_error_response(client_conn)
        except* UpstreamConnectionTimeout:
            if client_conn.messages_read != upstream_conn.messages_read:
                UPSTREAM_TIMEOUTS.inc()
                logger.error("Timeout on getting data from upstream")
        except* Exception as exc:
            for e in exc.exceptions:
                logger.exception(e)

    async def measure_latency(self, client_conn: BaseConnection, upstream_conn: BaseConnection) -> None:
        while True:
            request_time = await client_conn.messages_read_timestamps.get()
            response_time = await upstream_conn.messages_read_timestamps.get()
            latency = response_time - request_time
            REQUEST_LATENCY.labels(upstream=upstream_conn.addr).observe(latency)

    async def client_to_upstream(self, client_conn: BaseConnection, upstream_conn: BaseConnection) -> None:
        logger.info("Getting data from client...")
        async for data in client_conn.iterator():
            await upstream_conn.write(data)

    async def upstream_to_client(self, client_conn: BaseConnection, upstream_conn: BaseConnection) -> None:
        logger.info("Sending response to client...")
        async for data in upstream_conn.iterator():
            await self.send_response(client_conn, data)

    async def send_response(self, client_conn: BaseConnection, response: bytes):
        try:
            await client_conn.write(response)
        except ClientConnectionClosed:
            pass

    async def send_parsing_error_response(self, client_conn: BaseConnection) -> None:
        error = get_error_response(400, "Bad Request", "Invalid request")
        await self.send_response(client_conn, error.full)

    async def send_bad_gateway_response(self, client_conn: BaseConnection):
        error = get_error_response(502, "Bad Gateway", "Internal error")
        await self.send_response(client_conn, error.full)

    async def start_server(self) -> None:
        await self.pool.prepare_connections()
        host, port = self.config.listen.split(":")
        server = await asyncio.start_server(self.client_handler, host=host, port=port)
        async with server:
            logger.info(f"Starting server host={host} port={port}")
            await server.serve_forever()
