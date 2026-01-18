import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class EchoHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    # close_connection = False

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        self.wfile.write(body)
        # self.wfile.close()


if __name__ == "__main__":
    server = ThreadingHTTPServer(("localhost", int(sys.argv[1])), EchoHandler)
    print(f"HTTP Echo сервер запущен на порту {sys.argv[1]}")
    server.serve_forever()
