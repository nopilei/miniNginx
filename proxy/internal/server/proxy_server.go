// # import asyncio
// # import logging

// # from config import Config
// # from http.error_responses import get_error_response
// # from http_utils.external.base import BaseConnection
// # from http_utils.http_reader import HTTPParseError
// # from http_utils.external.upstream import UpstreamConnectionTimeout
// # from metrics import REQUEST_LATENCY, UPSTREAM_TIMEOUTS, POOL_TIMEOUTS
// # from upstream_pool import PoolConnectionError, RoundRobinUpstreamPool, PoolMember
// # from http_utils.external.client import ClientConnectionTimeout, ClientConnectionClosed, ClientConnection
// # from context import client_addr_var

// # logger = logging.getLogger(__name__)

// # class ProxyServer:
// #     def __init__(self, config: Config):
// #         self.config = config
// #         self.conn_semaphore = asyncio.Semaphore(config.limits.max_client_conns)
// #         self.total_timeout_s = self.config.timeouts.total_ms / 1000
// #         self.pool = RoundRobinUpstreamPool(config)

// #     async def start_server(self) -> None:
// #         await self.pool.prepare_connections()
// #         host, port = self.config.listen.split(":")
// #         server = await asyncio.start_server(self.client_handler, host=host, port=port)
// #         async with server:
// #             logger.info(f"Starting server host={host} port={port}")
// #             await server.serve_forever()

// #     async def client_handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
// #         async with self.conn_semaphore:
// #             client_connection = ClientConnection(reader, writer, self.config)
// #             token = client_addr_var.set(str(client_connection.addr))
// #             logger.info("Got new client connection.")
// #             try:
// #                 await asyncio.wait_for(self.process_client_connection(client_connection), self.total_timeout_s)
// #             except TimeoutError:
// #                 logger.info("Keep alive connection with client closed forcefully: too long session!")
// #             client_addr_var.reset(token)

// #     async def process_client_connection(self, client_conn: BaseConnection) -> None:
// #         logger.info('Processing new client connection.')

// #         try:
// #             await self.proxy_client(client_conn)
// #         except (ClientConnectionTimeout, ClientConnectionClosed):
// #             logger.info("Client timeout.")
// #         except UpstreamConnectionTimeout:
// #             logger.error("Upstream timeout")
// #             await self.send_bad_gateway_response(client_conn)
// #         except HTTPParseError:
// #             logger.error("Error parsing client http data")
// #             await self.send_parsing_error_response(client_conn)
// #         except PoolConnectionError as exc:
// #             POOL_TIMEOUTS.inc()
// #             logger.error(exc)
// #             await self.send_bad_gateway_response(client_conn)
// #         except Exception as exc:
// #             logger.error(exc)
// #         finally:
// #             await client_conn.close()

// #     async def cleanup(self, pool_member: PoolMember, task: asyncio.Task) -> None:
// #         if task:
// #             try:
// #                 await task
// #             except UpstreamConnectionTimeout:
// #                 if not pool_member.response_is_read:
// #                     UPSTREAM_TIMEOUTS.labels(upstream=pool_member.connection.addr).inc()
// #                     raise
// #             finally:
// #                 await self.pool.release(pool_member, is_healthy=pool_member.response_is_read)

// #     async def proxy_client(self, client_conn: BaseConnection) -> None:
// #         logger.info("Getting data from client...")

// #         pool_member, response_task = None, None
// #         loop = asyncio.get_event_loop()
// #         try:
// #             async for data in client_conn.iterator():
// #                 if data.is_message_start:
// #                     start_time = loop.time()
// #                     await self.cleanup(pool_member, response_task)
// #                     pool_member = await self.pool.acquire()
// #                     logger.info(f"Got upstream connection: {pool_member.connection.addr}")
// #                     response_task = asyncio.create_task(self.upstream_to_client(client_conn, pool_member, start_time))

// #                 await pool_member.connection.write(data.chunk)
// #         finally:
// #             await self.cleanup(pool_member, response_task)

// #     async def upstream_to_client(self, client_conn: BaseConnection, pool_member: PoolMember, start_time: float) -> None:
// #         logger.info("Sending response to client...")
// #         async for data in pool_member.connection.iterator():
// #             await self.send_response(client_conn, data.chunk)
// #             if data.is_message_end:
// #                 end_time = asyncio.get_event_loop().time()
// #                 REQUEST_LATENCY.labels(upstream=pool_member.connection.addr).observe(end_time - start_time)
// #                 await self.pool.release(pool_member, is_healthy=True)
// #                 return

// #     async def send_response(self, client_conn: BaseConnection, response: bytes) -> None:
// #         try:
// #             await client_conn.write(response)
// #         except ClientConnectionClosed:
// #             pass

// #     async def send_parsing_error_response(self, client_conn: BaseConnection) -> None:
// #         error = get_error_response(400, "Bad Request", "Invalid request")
// #         await self.send_response(client_conn, error)

// #     async def send_bad_gateway_response(self, client_conn: BaseConnection) -> None:
// #         error = get_error_response(502, "Bad Gateway", "Internal error")
// #         await self.send_response(client_conn, error)

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
	err := s.pool.PrepareConnections()
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", s.config.Listen)
	sugaredLogger := s.logger.Sugar()

	sugaredLogger.Info("Starting server ", s.config.Listen)
	for {
		conn, err := ln.Accept()
		if err != nil {
			sugaredLogger.Error(err)
			continue
		}
		sugaredLogger.Info("New connection from: ", conn.RemoteAddr())

		s.connSemaphore <- struct{}{}
		go s.ClientHandler(conn)
	}
}

func (s *ProxyServer) ClientHandler(conn net.Conn) {
	defer func() { <-s.connSemaphore }()

	clientConnection := node.NewClientConnection(conn, s.config)
	logger := s.logger.With(zap.String("client_addr", conn.RemoteAddr().String()))
	logger.Info("Got new client connection")
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.totalTimeoutS)*time.Second)
	defer cancel()

	s.ProcessClientConnection(ctx, logger, clientConnection)
}

func (s *ProxyServer) ProcessClientConnection(ctx context.Context, logger *zap.Logger, clientConnection *node.Connection) error {
	defer clientConnection.Close()
	logger.Info("Processing new client connection.")

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
		// POOL_TIMEOUTS.inc()
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
	logger.Info("Getting data from client...")

	var (
		poolMember  *upstream.PoolMember
		upstreamRes chan error
	)
	defer func() { s.CleanUp(ctx, logger, poolMember, upstreamRes) }()

	for chunk, err := range clientConn.Iterator(ctx) {
		if err != nil {
			return err
		}
		// TODO: add metrics
		if chunk.IsMessageStart {
			poolMember, err = s.pool.Acquire()
			if err != nil {
				return err
			}

			upstreamRes = make(chan error, 1)
			go func(pm *upstream.PoolMember, up chan error) {
				up <- s.SendUpstreamResponse(ctx, logger, clientConn, pm)
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
		s.pool.Release(poolMember, logger, poolMember.ResponseIsRead())
		return ctx.Err()
	case err := <-upstreamResCh:
		s.pool.Release(poolMember, logger, poolMember.ResponseIsRead())
		return err
	}
}

func (s *ProxyServer) SendUpstreamResponse(ctx context.Context, logger *zap.Logger, clientConn *node.Connection, poolMember *upstream.PoolMember) error {
	logger.Info("Sending response to client...")
	for chunk, err := range poolMember.Iterator(ctx) {
		if err != nil {
			return err
		}
		s.SendResponse(clientConn, logger, chunk.Chunk)
		if chunk.IsMessageEnd {
			// TODO add metrics
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
