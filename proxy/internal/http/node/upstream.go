package node

import (
	"net"
	"proxy/config"
	"proxy/internal/http/stream"
)

type UpstreamConnectionClosedError struct{}

func (e UpstreamConnectionClosedError) Error() string {
	return "Upstream connection closed"
}

type UpstreamTimeoutError struct{}

func (e UpstreamTimeoutError) Error() string {
	return "Upstream connection timeout"
}

func NewUpstreamConnection(conn net.Conn, config config.Config) *Connection {
	return &Connection{
		conn:                  conn,
		readTimeoutS:          config.Timeouts.ReadMs / 1000,
		writeTimeoutS:         config.Timeouts.WriteMs / 1000,
		iterator:              newUpstreamIterator(conn, config.Timeouts.ReadMs/1000),
		connectionClosedError: UpstreamConnectionClosedError{},
	}
}

func newUpstreamIterator(conn net.Conn, readTimeoutS int) *HTTPIterator {
	return &HTTPIterator{
		conn:         conn,
		readTimeoutS: readTimeoutS,
		messagesRead: 0,
		reader:       stream.NewResponseReader(conn),
		timeoutError: UpstreamTimeoutError{},
	}

}
