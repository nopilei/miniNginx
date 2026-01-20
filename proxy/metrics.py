import asyncio

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST


UPSTREAM_TIMEOUTS = Counter(
    "proxy_upstream_errors_total",
    "Upstream timeouts",
    ["upstream"]
)

REQUEST_LATENCY = Histogram(
    "proxy_request_latency_seconds",
    "HTTP request latency",
    ["upstream"],
    buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10)
)


async def handle_metrics(reader, writer):
    try:
        await reader.readuntil(b"\r\n\r\n")
    except asyncio.IncompleteReadError:
        writer.close()
        return

    body = generate_latest()
    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: " + CONTENT_TYPE_LATEST.encode() + b"\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"Connection: close\r\n\r\n"
        + body
    )

    writer.write(response)
    await writer.drain()
    writer.close()


async def start_metrics_server():
    server = await asyncio.start_server(handle_metrics, "0.0.0.0", 9100)
    async with server:
        await server.serve_forever()
