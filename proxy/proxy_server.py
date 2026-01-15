import asyncio
import logging

from config import ParsedConfig
from http_utils.error_responses import get_error_response
from http_utils.external.base import BaseConnection
from http_utils.http_parser import HTTPParseError
from http_utils.external.upstream import UpstreamConnectionTimeout
from upstream_pool import PoolConnectionError, RoundRobinUpstreamPool
from http_utils.external.client import ClientConnectionTimeout, ClientConnectionClosed, ClientConnection
from context import client_addr_var


logger = logging.getLogger(__name__)

class ProxyServer:
    def __init__(self, config: ParsedConfig):
        self.config = config
        self.pool = RoundRobinUpstreamPool(config)

    async def client_handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_connection = ClientConnection(reader, writer)
        token = client_addr_var.set(writer.get_extra_info('socket').getpeername())
        logger.info('Got new client connection.')
        await self.process_client_connection(client_connection)
        client_addr_var.reset(token)

    async def send_response(self, client_connection: BaseConnection, response: bytes):
        try:
            await client_connection.write(response)
        except ClientConnectionClosed:
            pass

    async def process_client_connection(self, client_connection: BaseConnection):
        logger.info('Processing new client connection.')
        try:
            upstream_connection, upstream_connections = await self.pool.acquire()
        except PoolConnectionError as exc:
            logger.error(exc)
            await self.send_bad_gateway_response(client_connection)
            await client_connection.close()
            return

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.client_to_upstream(client_connection, upstream_connection))
                tg.create_task(self.upstream_to_client(client_connection, upstream_connection))
        except* (ClientConnectionTimeout, ClientConnectionClosed):
            logger.info("Client timeout.")
        except* HTTPParseError as exc:
            logger.error("Error parsing http data")
            await self.send_parsing_error_response(client_connection)
        except* UpstreamConnectionTimeout as exc:
            if client_connection.messages_read != upstream_connection.messages_read:
                for e in exc.exceptions:
                    logger.exception(e)
                await self.send_bad_gateway_response(client_connection)
        except* Exception as exc:
            for e in exc.exceptions:
                logger.exception(e)
            await self.send_bad_gateway_response(client_connection)
        finally:
            await client_connection.close()
            await self.pool.release(upstream_connections, upstream_connection, client_connection.messages_read == upstream_connection.messages_read)

    async def client_to_upstream(self, client_connection: BaseConnection, upstream_connection: BaseConnection):
        logger.info("Getting data from client...")
        async for data in client_connection.iterator():
            await upstream_connection.write(data)

    async def upstream_to_client(self, client_connection: BaseConnection, upstream_connection: BaseConnection):
        logger.info("Sending response to client...")
        async for data in upstream_connection.iterator():
            await self.send_response(client_connection, data)

    async def send_parsing_error_response(self, client_connection):
        error = get_error_response(400, 'Bad Request', 'Invalid request')
        await self.send_response(client_connection, error.full)

    async def send_bad_gateway_response(self, client_connection):
        error = get_error_response(502, 'Bad Gateway', "Internal error")
        await self.send_response(client_connection, error.full)

    async def start_server(self):
        await self.pool.prepare_connections()
        host, port = self.config.listen.split(":")
        server = await asyncio.start_server(self.client_handler, host=host, port=port)
        async with server:
            logger.info(f"Starting server host={host} port={port}")
            await server.serve_forever()
