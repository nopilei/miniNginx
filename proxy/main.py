# import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from concurrent.futures import wait

from logger import setup_logging
from metrics import start_metrics_server
from proxy_server import ProxyServer

from config import ConfigLoader


def run(config_path: str):
    setup_logging()
    config = ConfigLoader(config_path).get_config()
    proxy_server = ProxyServer(config)
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(proxy_server.start_server)
        executor.submit(start_metrics_server)


if __name__ == '__main__':
    run(sys.argv[1])
