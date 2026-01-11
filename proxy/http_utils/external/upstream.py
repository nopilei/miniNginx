import asyncio

from http_utils.external.base import BaseHTTPIterator, BaseConnection
from http_utils.http_parser import HTTPResponseReader


class UpstreamConnectionTimeout(TimeoutError):
    pass


class UpstreamConnectionClosed(ConnectionResetError):
    pass


class UpstreamResponseIterator(BaseHTTPIterator):
    http_reader_class = HTTPResponseReader
    timeout_err = UpstreamConnectionTimeout("Timeout on getting data from resource")


class UpstreamConnection(BaseConnection):
    connection_closed_err = UpstreamConnectionClosed("Upstream closed connection")
    http_iterator_class = UpstreamResponseIterator
