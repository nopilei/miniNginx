from http_utils.http_parser import HTTPResponse


def get_error_response(status: int, reason: str, body: str) -> HTTPResponse:
    return HTTPResponse(
        version=b'HTTP/1.1',
        status=str(status).encode(),
        reason=reason.encode(),
        body=body.encode(),
        headers={
            b'Content-Length': str(len(body.encode())).encode(),
        }
    )
