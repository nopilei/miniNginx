package upstream

import (
	"context"
	"proxy/internal/http/node"
)

type UpstreamConnectionQueue struct {
	queue chan *node.Connection
}

func NewUpstreamConnectionQueue(size int) *UpstreamConnectionQueue {
	return &UpstreamConnectionQueue{
		queue: make(chan *node.Connection, size),
	}
}

func (q *UpstreamConnectionQueue) Put(conn *node.Connection) {
	q.queue <- conn
}

func (q *UpstreamConnectionQueue) Get(ctx context.Context) (*node.Connection, error) {
	select {
	case conn := <-q.queue:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (q *UpstreamConnectionQueue) Len() int {
	return len(q.queue)
}
