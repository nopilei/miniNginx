import asyncio
import logging

from config import Config
from http_utils.error_responses import get_error_response
from http_utils.external.base import BaseConnection
from http_utils.http_reader import HTTPParseError
from http_utils.external.upstream import UpstreamConnectionTimeout
from metrics import REQUEST_LATENCY, UPSTREAM_TIMEOUTS, POOL_TIMEOUTS
from upstream_pool import PoolConnectionError, RoundRobinUpstreamPool, PoolMember
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
            client_addr_var.reset(token)

    async def process_client_connection(self, client_conn: BaseConnection) -> None:
        logger.info('Processing new client connection.')

        try:
            await self.proxy_client(client_conn)
        except (ClientConnectionTimeout, ClientConnectionClosed):
            logger.info("Client timeout.")
        except UpstreamConnectionTimeout:
            logger.error("Upstream timeout")
            await self.send_bad_gateway_response(client_conn)
        except HTTPParseError:
            logger.error("Error parsing client http data")
            await self.send_parsing_error_response(client_conn)
        except PoolConnectionError as exc:
            POOL_TIMEOUTS.inc()
            logger.error(exc)
            await self.send_bad_gateway_response(client_conn)
        except Exception as exc:
            logger.error(exc)
        finally:
            await client_conn.close()

    async def cleanup(self, pool_member: PoolMember, task: asyncio.Task) -> None:
        if task:
            try:
                await task
            except UpstreamConnectionTimeout:
                if not pool_member.response_is_read:
                    UPSTREAM_TIMEOUTS.labels(upstream=pool_member.connection.addr).inc()
                    raise
            finally:
                await self.pool.release(pool_member, is_healthy=pool_member.response_is_read)

    async def proxy_client(self, client_conn: BaseConnection) -> None:
        logger.info("Getting data from client...")

        pool_member, task = None, None
        loop = asyncio.get_event_loop()
        try:
            async for data in client_conn.iterator():
                if data.is_message_start:
                    start_time = loop.time()
                    await self.cleanup(pool_member, task)
                    pool_member = await self.pool.acquire()
                    logger.info(f"Got upstream connection: {pool_member.connection.addr}")
                    task = asyncio.create_task(self.upstream_to_client(client_conn, pool_member, start_time))

                await pool_member.connection.write(data.chunk)
        finally:
            await self.cleanup(pool_member, task)

    async def upstream_to_client(self, client_conn: BaseConnection, pool_member: PoolMember, start_time: float) -> None:
        logger.info("Sending response to client...")
        async for data in pool_member.connection.iterator():
            await self.send_response(client_conn, data.chunk)
            if data.is_message_end:
                end_time = asyncio.get_event_loop().time()
                REQUEST_LATENCY.labels(upstream=pool_member.connection.addr).observe(end_time - start_time)
                await self.pool.release(pool_member, is_healthy=True)
                return

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
