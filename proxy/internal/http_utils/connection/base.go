package connection

import (
	"iter"
	"net"
	httputils "proxy/internal/http_utils"
	"strconv"
    "proxy/config"
	"time"
)

type Connection struct {
	conn          net.Conn
	readTimeoutS  int
	writeTimeoutS int
	iterator      HTTPIterator
    connectionClosedError error
}
func NewClientConnection(conn net.Conn, config config.Config) Connection {
    return Connection{
        conn:          conn,    
        readTimeoutS:  config.Timeouts.ReadMs / 1000,
        writeTimeoutS: config.Timeouts.WriteMs / 1000,
        iterator:      NewClientIterator(conn,  config.Timeouts.ReadMs / 1000),
        connectionClosedError: ClientConnectionClosedError{},
    }
}
func NewUpstreamConnection(conn net.Conn, config config.Config) Connection {
    return Connection{
        conn:          conn,    
        readTimeoutS:  config.Timeouts.ReadMs / 1000,
        writeTimeoutS: config.Timeouts.WriteMs / 1000,
        iterator:      NewUpstreamIterator(conn,  config.Timeouts.ReadMs / 1000),
        connectionClosedError: UpstreamConnectionClosedError{},
    }
}


func (c Connection) Iterator() iter.Seq2[httputils.Chunk, error] {
    return c.iterator.All()
}
func (c Connection) Addr() (string, int, error) {
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
func (c Connection) Write(response []byte) error {
    c.conn.SetWriteDeadline(time.Now().Add(time.Duration(c.writeTimeoutS) * time.Second))
    _, err := c.conn.Write(response)
    if err != nil {
        return c.connectionClosedError
    }
    return nil
}
func (c Connection) Close() error {
    err := c.conn.Close()
    if err != nil {
        return c.connectionClosedError
    }
    return nil
}

func (c Connection) MessagesRead() int {
    return c.iterator.MessagesRead()
}


type ClientConnectionClosedError struct{}

func (e ClientConnectionClosedError) Error() string {
	return "Client connection closed"
}

type UpstreamConnectionClosedError struct{}

func (e UpstreamConnectionClosedError) Error() string {
	return "Upstream connection closed"
}

type ClientTimeoutError struct{}

func (e ClientTimeoutError) Error() string {
	return "Client connection timeout"
}

type UpstreamTimeoutError struct{}

func (e UpstreamTimeoutError) Error() string {
	return "Upstream connection timeout"
}

type HTTPIterator struct {
	conn         net.Conn
	readTimeoutS int
	messagesRead int
	reader       httputils.Reader
	timeoutError error
}

func (i HTTPIterator) All() iter.Seq2[httputils.Chunk, error] {
	return func(yield func(httputils.Chunk, error) bool) {
		i.SetTimeout()
		for chunk, err := range i.reader.All() {
			if err == nil {
				i.SetTimeout()
				if chunk.IsMessageEnd {
					i.messagesRead++
				}
				if !yield(chunk, nil) {
					return
				}
				continue
			}

			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() {
				yield(chunk, i.timeoutError)
				return
			} else {
				yield(chunk, err)
				return
			}
		}
	}
}
func (i HTTPIterator) MessagesRead() int {
	return i.messagesRead
}

func (i HTTPIterator) SetTimeout() {
	i.conn.SetReadDeadline(time.Now().Add(time.Duration(i.readTimeoutS) * time.Second))
}

func NewClientIterator(conn net.Conn, readTimeoutS int) HTTPIterator {
	return HTTPIterator{
		conn:         conn,
		readTimeoutS: readTimeoutS,
		messagesRead: 0,
		reader:       httputils.NewRequestReader(conn),
		timeoutError: ClientTimeoutError{},
	}
}
func NewUpstreamIterator(conn net.Conn, readTimeoutS int) HTTPIterator {
	return HTTPIterator{
		conn:         conn,
		readTimeoutS: readTimeoutS,
		messagesRead: 0,
		reader:       httputils.NewResponseReader(conn),
		timeoutError: UpstreamTimeoutError{},
	}

}
