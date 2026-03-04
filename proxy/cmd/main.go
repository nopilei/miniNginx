// import asyncio
// import sys

// from logger import setup_logging
// from metrics import start_metrics_server
// from proxy_server import ProxyServer

// from config import ConfigLoader

// async def run(config_path: str):
//     setup_logging()
//     config = ConfigLoader(config_path).get_config()
//     await asyncio.gather(
//         ProxyServer(config).start_server(),
//         start_metrics_server()
//     )

// if __name__ == '__main__':
//     asyncio.run(run(sys.argv[1]))

package main

import (
	"os"
	"proxy/config"
	"proxy/internal/server"
	"sync"
)

func main() { 
    var wg sync.WaitGroup

    if len(os.Args) < 2 {
        panic("Config path not provided")
    }

    serverConfig, err := config.ConfigLoader{Path: os.Args[1]}.GetConfig()
    if err != nil {
        panic(err)
    }

    logger, err := config.GetLogger()
    if err != nil {
        panic(err)
    }
    
    proxyServer := server.New(serverConfig, logger)
    wg.Go(func() {proxyServer.StartServer)

    err = proxyServer.StartServer()
    if err != nil {
        panic(err)
    }
}