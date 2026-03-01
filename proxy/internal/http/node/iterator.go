package node

import (
	"context"
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

func (i *HTTPIterator) All(ctx context.Context) iter.Seq2[stream.Chunk, error] {
	go func(){
		<-ctx.Done()
		i.conn.SetReadDeadline(time.Now())
	}()

	return func(yield func(stream.Chunk, error) bool) {
		i.SetTimeout()
		for chunk, err := range i.reader.All(ctx) {
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

			if ctx.Err() != nil {
				yield(chunk, ctx.Err())
				return
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
