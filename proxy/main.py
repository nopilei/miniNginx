# import asyncio
import sys
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
from concurrent.futures import wait
from multiprocessing import Process
import socket

from logger import setup_logging
from metrics import start_metrics_server
from proxy_server import ProxyServer

from config import ConfigLoader, Config


def run(config: Config, server_sock: socket.socket):
    setup_logging()
    proxy_server = ProxyServer(config, server_sock)
    proxy_server.start_server()


if __name__ == '__main__':
    config = ConfigLoader(sys.argv[1]).get_config()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    host, port = config.listen.split(":")
    server_sock.bind((host, int(port)))
    server_sock.listen()

    processes = []
    for _ in range(config.workers):
        p = Process(target=run, args=(config, server_sock))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
