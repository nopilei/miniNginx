import asyncio
import sys

from logger import setup_logging
from metrics import start_metrics_server
from proxy_server import ProxyServer

from config import ConfigLoader


async def run(config_path: str):
    setup_logging()
    config = ConfigLoader(config_path).get_config()
    await asyncio.gather(
        ProxyServer(config).start_server(),
        start_metrics_server()
    )

if __name__ == '__main__':
    asyncio.run(run(sys.argv[1]))
