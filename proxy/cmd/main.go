package main

import (
	"os"
	"proxy/config"
	"proxy/internal/server"

    "golang.org/x/sync/errgroup"
)

func main() { 
    wg := new(errgroup.Group)

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

    wg.Go(func() error {return proxyServer.StartServer()})
    wg.Go(func() error {return config.StartMetricsServer(logger)})
    err = wg.Wait()
    if err != nil {
        panic(err)
    }
}