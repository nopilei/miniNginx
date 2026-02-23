package node

import (
	"net"
	"proxy/config"
	"proxy/internal/http/stream"
)

type ClientConnectionClosedError struct{}

func (e ClientConnectionClosedError) Error() string {
	return "Client connection closed"
}

type ClientTimeoutError struct{}

func (e ClientTimeoutError) Error() string {
	return "Client connection timeout"
}

func NewClientConnection(conn net.Conn, config config.Config) *Connection {
	return &Connection{
		conn:                  conn,
		readTimeoutS:          config.Timeouts.ReadMs / 1000,
		writeTimeoutS:         config.Timeouts.WriteMs / 1000,
		iterator:              newClientIterator(conn, config.Timeouts.ReadMs/1000),
		connectionClosedError: ClientConnectionClosedError{},
	}
}

func newClientIterator(conn net.Conn, readTimeoutS int) *HTTPIterator {
	return &HTTPIterator{
		conn:         conn,
		readTimeoutS: readTimeoutS,
		messagesRead: 0,
		reader:       stream.NewRequestReader(conn),
		timeoutError: ClientTimeoutError{},
	}
}
