import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

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
    def __init__(self, config: Config, server_sock: socket.socket):
        self.config = config
        self.total_timeout_s = self.config.timeouts.total_ms / 1000
        self.executor = ThreadPoolExecutor(max_workers=self.config.limits.max_client_conns)
        self.shutdown_event = threading.Event()
        self.server_sock = server_sock
        self.pool = RoundRobinUpstreamPool(config)

    def start_server(self) -> None:
        self.pool.prepare_connections()

        logger.info(f"Starting server")
        try:
            while not self.shutdown_event.is_set():
                try:
                    client_sock, addr = self.server_sock.accept()
                except OSError:
                    # Уже закрыли server_sock
                    break
                try:
                    self.executor.submit(self.client_handler, client_sock)
                except RuntimeError:
                    # executor уже shutdown
                    client_sock.close()
        finally:
            self.shutdown()

    def shutdown(self):
        self.shutdown_event.set()
        self.server_sock.close()  # разбудит accept()
        self.executor.shutdown(wait=False)

    def client_handler(self, sock: socket.socket) -> None:
        client_connection = ClientConnection(sock, self.config)
        token = client_addr_var.set(str(client_connection.addr))
        logger.info("Got new client connection.")
        self.process_client_connection(client_connection)
        client_addr_var.reset(token)

    def process_client_connection(self, client_conn: BaseConnection) -> None:
        logger.info('Processing new client connection.')

        try:
            self.proxy_client(client_conn)
        except (ClientConnectionTimeout, ClientConnectionClosed):
            logger.info("Client timeout.")
        except UpstreamConnectionTimeout:
            logger.error("Upstream timeout")
            self.send_bad_gateway_response(client_conn)
        except HTTPParseError:
            logger.error("Error parsing client http data")
            self.send_parsing_error_response(client_conn)
        except PoolConnectionError as exc:
            POOL_TIMEOUTS.inc()
            logger.error(exc)
            self.send_bad_gateway_response(client_conn)
        except Exception as exc:
            logger.error(exc)
        finally:
            client_conn.close()

    def cleanup(self, pool_member: PoolMember, task: threading.Thread) -> None:
        if task:
            try:
                task.join()
            except UpstreamConnectionTimeout:
                if not pool_member.response_is_read:
                    UPSTREAM_TIMEOUTS.labels(upstream=pool_member.connection.addr).inc()
                    raise
            finally:
                self.pool.release(pool_member, is_healthy=pool_member.response_is_read)

    def proxy_client(self, client_conn: BaseConnection) -> None:
        logger.info("Getting data from client...")

        pool_member, response_thread = None, None
        try:
            for data in client_conn.iterator():
                if data.is_message_start:
                    start_time = time.monotonic()
                    self.cleanup(pool_member, response_thread)
                    pool_member = self.pool.acquire()
                    logger.info(f"Got upstream connection: {pool_member.connection.addr}")
                    response_thread = threading.Thread(
                        target=self.upstream_to_client,
                        args=(client_conn, pool_member, start_time),
                        daemon=True,
                    )
                    response_thread.start()

                pool_member.connection.write(data.chunk)
        except Exception as e:
            raise e
        finally:
            self.cleanup(pool_member, response_thread)

    def upstream_to_client(self, client_conn: BaseConnection, pool_member: PoolMember, start_time: float) -> None:
        logger.info("Sending response to client...")
        for data in pool_member.connection.iterator():
            self.send_response(client_conn, data.chunk)
            if data.is_message_end:
                end_time = time.monotonic()
                REQUEST_LATENCY.labels(upstream=pool_member.connection.addr).observe(end_time - start_time)
                self.pool.release(pool_member, is_healthy=True)
                return

    def send_response(self, client_conn: BaseConnection, response: bytes) -> None:
        try:
            client_conn.write(response)
        except ClientConnectionClosed:
            pass

    def send_parsing_error_response(self, client_conn: BaseConnection) -> None:
        error = get_error_response(400, "Bad Request", "Invalid request")
        self.send_response(client_conn, error)

    def send_bad_gateway_response(self, client_conn: BaseConnection) -> None:
        error = get_error_response(502, "Bad Gateway", "Internal error")
        self.send_response(client_conn, error)
