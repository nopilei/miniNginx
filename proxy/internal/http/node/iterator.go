package node

import (
	"iter"
	"net"
	"proxy/internal/http/stream"
	"time"
)

type HTTPIterator struct {
	conn         net.Conn
	readTimeoutS int
	messagesRead int
	reader       *stream.Reader
	timeoutError error
}

func (i *HTTPIterator) All() iter.Seq2[stream.Chunk, error] {
	return func(yield func(stream.Chunk, error) bool) {
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
func (i *HTTPIterator) MessagesRead() int {
	return i.messagesRead
}

func (i *HTTPIterator) SetTimeout() {
	i.conn.SetReadDeadline(time.Now().Add(time.Duration(i.readTimeoutS) * time.Second))
}
