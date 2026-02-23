package upstream

import (
	"context"
	"fmt"
	"net"
	"proxy/config"
	"proxy/internal/http/node"
	"time"
)

type PoolMember struct {
	*node.Connection
	upstreamQueue *UpstreamConnectionQueue
	isReturned    bool
}

func (m *PoolMember) ResponseIsRead() bool {
	return m.MessagesRead() == 1
}

type RoundRobinPool struct {
	config          config.Config
	upstreamAddrs   []config.UpstreamConfig
	connectTimeoutS int
	upstreamQueues  chan *UpstreamConnectionQueue
}

func NewPool(config config.Config) *RoundRobinPool {
	return &RoundRobinPool{
		config:          config,
		upstreamAddrs:   config.Upstreams,
		connectTimeoutS: config.Timeouts.ConnectMs / 1000,
		upstreamQueues:  make(chan *UpstreamConnectionQueue, len(config.Upstreams)),
	}
}

func (p *RoundRobinPool) PrepareConnections() error {
	for _, upstreamAddr := range p.upstreamAddrs {
		host, port := upstreamAddr.Host, upstreamAddr.Port
		maxConns := p.config.Limits.MaxConnsPerUpstream
		connectionQueue := NewUpstreamConnectionQueue(maxConns)

		for range maxConns {
			conn, err := p.ConnectUpstream(host, port)
			if err != nil {
				// logger.exc
				break
			}
			connectionQueue.Put(conn)
		}
		if connectionQueue.Len() > 0 {
			p.upstreamQueues <- connectionQueue
		}
	}
	if len(p.upstreamQueues) == 0 {
		return PoolConnectionError{"Failed connect to upstreamQueues"}
	}
	return nil
}

func (p *RoundRobinPool) Acquire() (*PoolMember, error) {
	upstreamQueue := <-p.upstreamQueues
	p.upstreamQueues <- upstreamQueue

	// TODO: metrics
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(p.connectTimeoutS))
	defer cancel()
	connection, err := upstreamQueue.Get(ctx)
	if err != nil {
		return nil, PoolConnectionError{"Timeout on getting upstream from pool"}
	}
	return &PoolMember{upstreamQueue: upstreamQueue, Connection: connection}, nil
}

func (p *RoundRobinPool) Release(poolMember *PoolMember, isHealthy bool) {
	if poolMember.isReturned {
		return
	}
	queue, connection := poolMember.upstreamQueue, poolMember.Connection
	if isHealthy {
		queue.Put(connection)
	} else {
		host, port, _ := connection.Addr()
		conn, err := p.ConnectUpstream(host, port)
		if err != nil {
			// logger.exc
			return
		}
		queue.Put(conn)
	}
	poolMember.isReturned = true
}

func (p *RoundRobinPool) ConnectUpstream(host string, port int) (*node.Connection, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%v:%v", host, port), time.Duration(p.connectTimeoutS))
	if err != nil {
		return nil, err
	}
	return node.NewUpstreamConnection(conn, p.config), nil

}
