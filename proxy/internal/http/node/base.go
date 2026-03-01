package node

import (
	"context"
	"iter"
	"net"
	"proxy/internal/http/stream"
	"time"
)

type Connection struct {
	conn                  net.Conn
	readTimeoutS          int
	writeTimeoutS         int
	iterator              *HTTPIterator
	connectionClosedError error
}

func (c *Connection) Iterator(ctx context.Context) iter.Seq2[stream.Chunk, error] {
	return c.iterator.All(ctx)
}
func (c *Connection) Addr() string {
	return c.conn.RemoteAddr().String()
}
func (c *Connection) Write(response []byte) error {
	c.conn.SetWriteDeadline(time.Now().Add(time.Duration(c.writeTimeoutS) * time.Second))
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
