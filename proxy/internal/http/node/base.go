package node

import (
	"iter"
	"net"
	"proxy/internal/http/stream"
	"strconv"
	"time"
)

type Connection struct {
	conn                  net.Conn
	readTimeoutS          int
	writeTimeoutS         int
	iterator              *HTTPIterator
	connectionClosedError error
}

func (c *Connection) Iterator() iter.Seq2[stream.Chunk, error] {
	return c.iterator.All()
}
func (c *Connection) Addr() (string, int, error) {
	addr := c.conn.RemoteAddr().String()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}
func (c *Connection) Write(response []byte) error {
	c.conn.SetWriteDeadline(time.Now().Add(time.Duration(c.writeTimeoutS)))
	_, err := c.conn.Write(response)
	if err != nil {
		return c.connectionClosedError
	}
	return nil
}
func (c *Connection) Close() error {
	err := c.conn.Close()
	if err != nil {
		return c.connectionClosedError
	}
	return nil
}

func (c *Connection) MessagesRead() int {
	return c.iterator.MessagesRead()
}
