from dataclasses import dataclass


@dataclass
class HTTPResponse:
    version: bytes
    status: bytes
    reason: bytes
    headers: dict[bytes, bytes]
    body: bytes

    def add_header(self, key: bytes, value: bytes) -> None:
        self.headers[key] = value

    @property
    def full(self) -> bytes:
        crlf = b'\r\n'
        request_line = b' '.join([self.version, self.status, self.reason])
        headers = crlf.join(k + b': ' + v for k, v in self.headers.items())
        body = self.body
        return request_line + crlf + headers + crlf * 2 + body


def get_error_response(status: int, reason: str, body: str) -> bytes:
    return HTTPResponse(
        version=b'HTTP/1.1',
        status=str(status).encode(),
        reason=reason.encode(),
        body=body.encode(),
        headers={
            b'Content-Length': str(len(body.encode())).encode(),
        }
    ).full
