from http_utils.external.base import BaseConnection, BaseHTTPIterator
from http_utils.http_reader import HTTPRequestReader


class ClientConnectionTimeout(TimeoutError):
    pass


class ClientConnectionClosed(ConnectionResetError):
    pass


class ClientRequestIterator(BaseHTTPIterator):
    http_reader_class = HTTPRequestReader
    timeout_err = ClientConnectionTimeout("Timeout on getting data from resource")


class ClientConnection(BaseConnection):
    connection_closed_err = ClientConnectionClosed("Client closed connection")
    http_iterator_class = ClientRequestIterator
