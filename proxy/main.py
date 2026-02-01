# import asyncio
import sys

from logger import setup_logging
from metrics import start_metrics_server
from proxy_server import ProxyServer

from config import ConfigLoader


def run(config_path: str):
    setup_logging()
    config = ConfigLoader(config_path).get_config()
    ProxyServer(config).start_server()


if __name__ == '__main__':
    run(sys.argv[1])
