import asyncio
import sys

from proxy_server import ProxyServer

from config import ConfigLoader


async def run(config_path: str):
    config = ConfigLoader(config_path).get_config()
    await ProxyServer(config).start_server()


if __name__ == '__main__':
    asyncio.run(run(sys.argv[1]))
