package server

import (
	"context"
	"errors"
	"net"
	"proxy/config"
	"proxy/internal/http/node"
	"proxy/internal/http/stream"
	"proxy/internal/upstream"
	"time"

	"go.uber.org/zap"
)

type ProxyServer struct {
	config        config.Config
	connSemaphore chan struct{}
	totalTimeoutS int
	pool          *upstream.RoundRobinPool
	logger        *zap.Logger
}

func New(config config.Config, logger *zap.Logger) *ProxyServer {
	return &ProxyServer{
		config:        config,
		connSemaphore: make(chan struct{}, config.Limits.MaxClientConns),
		totalTimeoutS: config.Timeouts.TotalMs / 1000,
		pool:          upstream.NewPool(config),
		logger:        logger,
	}
}

func (s *ProxyServer) StartServer() error {
	sugaredLogger := s.logger.Sugar()

	sugaredLogger.Info("Setting connections to upstreams... ")
	err := s.pool.PrepareConnections()
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", s.config.Listen)
	sugaredLogger.Info("Starting server ", s.config.Listen)
	for {
		conn, err := ln.Accept()
		if err != nil {
			sugaredLogger.Error(err)
			continue
		}
		sugaredLogger.Debug("New connection from: ", conn.RemoteAddr())

		s.connSemaphore <- struct{}{}
		go s.ClientHandler(conn)
	}
}

func (s *ProxyServer) ClientHandler(conn net.Conn) {
	defer func() { <-s.connSemaphore }()

	clientConnection := node.NewClientConnection(conn, s.config)
	logger := s.logger.With(zap.String("client_addr", conn.RemoteAddr().String()))
	logger.Debug("Got new client connection")
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.totalTimeoutS)*time.Second)
	defer cancel()

	s.ProcessClientConnection(ctx, logger, clientConnection)
}

func (s *ProxyServer) ProcessClientConnection(ctx context.Context, logger *zap.Logger, clientConnection *node.Connection) error {
	defer clientConnection.Close()
	logger.Debug("Processing new client connection.")

	err := s.ProxyClient(ctx, logger, clientConnection)

	switch err := err.(type) {
	case nil:
	case node.ClientTimeoutError, node.ClientConnectionClosedError:
		logger.Info("Client timeout.")
	case node.UpstreamTimeoutError:
		logger.Info("Upstream timeout")
		s.SendBadGatewayResponse(clientConnection, logger)
	case stream.ParseError:
		logger.Info("Error parsing client http data")
		s.SendParsingErrorResponse(clientConnection, logger)
	case upstream.PoolConnectionError:
		config.PoolTimeouts.Inc()
		logger.Info("Pool connection error: ", zap.Error(err))
		s.SendBadGatewayResponse(clientConnection, logger)
	default:
		if errors.Is(err, context.DeadlineExceeded) {
			logger.Info("Context deadline exceeded")
		} else {
			logger.Info("Unexpected error: ", zap.Error(err))
		}
	}
	return err

}

func (s *ProxyServer) ProxyClient(ctx context.Context, logger *zap.Logger, clientConn *node.Connection) error {
	logger.Debug("Getting data from client...")

	var (
		poolMember  *upstream.PoolMember
		upstreamRes chan error
	)
	defer func() { s.CleanUp(ctx, logger, poolMember, upstreamRes) }()

	for chunk, err := range clientConn.Iterator(ctx) {
		if err != nil {
			return err
		}
		startTime := time.Now()
		if chunk.IsMessageStart {
			poolMember, err = s.pool.Acquire()
			config.PoolLatency.Observe(time.Since(startTime).Seconds())
			if err != nil {
				return err
			}

			upstreamRes = make(chan error, 1)
			go func(pm *upstream.PoolMember, up chan error) {
				up <- s.SendUpstreamResponse(ctx, logger, clientConn, pm, startTime)
			}(poolMember, upstreamRes)
		}

		err = poolMember.Write(chunk.Chunk)
		if err != nil {
			return err
		}

		if chunk.IsMessageEnd {
			err = s.CleanUp(ctx, logger, poolMember, upstreamRes)
			if err != nil {
				return err
			}
			poolMember = nil
		}

	}
	return nil
}

func (s *ProxyServer) CleanUp(ctx context.Context, logger *zap.Logger, poolMember *upstream.PoolMember, upstreamResCh chan error) error {
	if poolMember == nil {
		return nil
	}
	if upstreamResCh == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		s.pool.Release(poolMember, logger, false)
		return ctx.Err()
	case err := <-upstreamResCh:
		if _, ok := err.(node.UpstreamTimeoutError); ok{
			config.UpstreamTimeouts.WithLabelValues(poolMember.Addr()).Inc()
		}
		s.pool.Release(poolMember, logger, err == nil)
		return err
	}
}

func (s *ProxyServer) SendUpstreamResponse(ctx context.Context, logger *zap.Logger, clientConn *node.Connection, poolMember *upstream.PoolMember, startTime time.Time) error {
	logger.Debug("Sending response to client...")
	for chunk, err := range poolMember.Iterator(ctx) {
		if err != nil {
			return err
		}
		s.SendResponse(clientConn, logger, chunk.Chunk)
		if chunk.IsMessageEnd {
			config.RequestLatency.WithLabelValues(poolMember.Addr()).Observe(time.Since(startTime).Seconds())
			return nil
		}
	}
	return nil
}

func (s *ProxyServer) SendResponse(clientConn *node.Connection, logger *zap.Logger, response []byte) {
	err := clientConn.Write(response)
	if err != nil {
		logger.Info("Client connection closed while sending response")
	}
}

func (s *ProxyServer) SendParsingErrorResponse(clientConn *node.Connection, logger *zap.Logger) {
	errorResponse := node.GetErrorResponse(400, "Bad Request", "Invalid request")
	s.SendResponse(clientConn, logger, errorResponse)
}

func (s *ProxyServer) SendBadGatewayResponse(clientConn *node.Connection, logger *zap.Logger) {
	errorResponse := node.GetErrorResponse(502, "Bad Gateway", "Internal error")
	s.SendResponse(clientConn, logger, errorResponse)
}
