import threading
import time
import socket

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


def monitor_active_threads(interval: float = 1.0):
    while True:
        active_count = threading.active_count()
        ACTIVE_TASKS.set(active_count)
        CPU_USER.set(process.cpu_times().user)
        MEM_RSS.set(process.memory_info().rss)
        MEM_VIRTUAL.set(process.memory_info().vms)
        time.sleep(interval)


def handle_metrics(sock: socket.socket):
    body = generate_latest()
    response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: " + CONTENT_TYPE_LATEST.encode() + (
                    b"\r\n"
                    b"Content-Length: " + str(len(body)).encode() +
                    b"\r\n"
                    b"Connection: close"
                    b"\r\n\r\n"
            )
            + body
    )

    sock.sendall(response)
    sock.close()


def start_metrics_server():
    threading.Thread(
        target=monitor_active_threads,
        daemon=True,
    ).start()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", 9100))
    server_sock.listen()

    while True:
        client_sock, addr = server_sock.accept()
        handle_metrics(client_sock)
