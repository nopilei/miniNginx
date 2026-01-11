import asyncio

from config import ParsedConfig
from http_utils.error_responses import get_error_response
from http_utils.external.base import BaseConnection
from http_utils.external.upstream import UpstreamConnectionTimeout
from http_utils.http_parser import HTTPParseError
from upstream_pool import PoolConnectionError, RoundRobinUpstreamPool
from http_utils.external.client import ClientConnectionTimeout, ClientConnectionClosed, ClientConnection


class ProxyServer:
    def __init__(self, config: ParsedConfig):
        self.config = config
        self.pool = RoundRobinUpstreamPool(config)

    async def client_handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        print('GOT COnn')
        client_connection = ClientConnection(reader, writer)
        asyncio.create_task(self.process_client_connection(client_connection))

    async def send_response(self, client_connection: BaseConnection, response: bytes):
        try:
            await client_connection.write(response)
        except ClientConnectionClosed:
            pass

    async def process_client_connection(self, client_connection: BaseConnection):
        try:
            upstream_connection, upstream_connections = await self.pool.acquire()
        except PoolConnectionError as exc:
            print(exc)
            await self.send_bad_gateway_response(client_connection, exc)
            await client_connection.close()
            return

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.client_to_upstream(client_connection, upstream_connection))
                tg.create_task(self.upstream_to_client(client_connection, upstream_connection))
        except* (ClientConnectionTimeout, ClientConnectionClosed):
            pass
        except* HTTPParseError as exc:
            message = [str(e) for e in exc.exceptions]
            print(message)
            await self.send_parsing_error_response(client_connection)
        except* Exception as exc:
            # raise exc
            message = [str(e) + '\n' for e in exc.exceptions]
            print(message)
            await self.send_bad_gateway_response(client_connection, message)
        else:
            await self.pool.release(upstream_connections, upstream_connection)
        finally:
            await client_connection.close()

    async def client_to_upstream(self, client_connection: BaseConnection, upstream_connection: BaseConnection):
        async for data in client_connection.iterator():
            await upstream_connection.write(data)

    async def upstream_to_client(self, client_connection: BaseConnection, upstream_connection: BaseConnection):
        async for data in upstream_connection.iterator():
            await self.send_response(client_connection, data)

    async def send_parsing_error_response(self, client_connection):
        error = get_error_response(400, 'Bad Request', 'Invalid request')
        await self.send_response(client_connection, error.full)

    async def send_bad_gateway_response(self, client_connection, exc):
        error = get_error_response(502, 'Bad Gateway', str(exc))
        await self.send_response(client_connection, error.full)

    async def start_server(self):
        await self.pool.prepare_connections()
        host, port = self.config.listen.split(":")
        server = await asyncio.start_server(self.client_handler, host=host, port=port)
        async with server:
            print(f"Starting server host={host} port={port}")
            await server.serve_forever()
