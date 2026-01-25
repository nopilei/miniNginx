import asyncio
import psutil

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST, Gauge

UPSTREAM_TIMEOUTS = Counter(
    "proxy_upstream_errors_total",
    "Upstream timeouts",
    ["upstream"]
)
POOL_TIMEOUTS = Counter(
    "proxy_pool_errors_total",
    "Pool timeouts",
)

REQUEST_LATENCY = Histogram(
    "proxy_request_latency_seconds",
    "HTTP request latency",
    ["upstream"],
    buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10)
)

POOL_LATENCY = Histogram(
    "pool_latency_seconds",
    "pool latency",
    buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10)
)

ACTIVE_TASKS = Gauge("active_tasks", "Number of active asyncio tasks")
CPU_USER = Gauge('process_cpu_seconds_total', 'CPU user time')
MEM_RSS = Gauge('process_resident_memory_bytes', 'Resident memory in bytes')
MEM_VIRTUAL = Gauge('process_virtual_memory_bytes', 'Virtual memory in bytes')

process = psutil.Process()


async def monitor_active_tasks(interval: float = 1.0):
    while True:
        tasks = asyncio.all_tasks(asyncio.get_event_loop())
        active_count = sum(1 for t in tasks if not t.done())
        ACTIVE_TASKS.set(active_count)
        CPU_USER.set(process.cpu_times().user)
        MEM_RSS.set(process.memory_info().rss)
        MEM_VIRTUAL.set(process.memory_info().vms)
        await asyncio.sleep(interval)


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
    asyncio.create_task(monitor_active_tasks())
    async with server:
        await server.serve_forever()
