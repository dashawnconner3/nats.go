package nats

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrConnectionClosed = errors.New("nats: connection closed")
	ErrBadSubscription  = errors.New("nats: invalid subscription")
)

type Conn struct {
	mu     sync.Mutex
	status int
	subs   map[int64]*Subscription
}

const (
	DISCONNECTED = iota
	CONNECTED
	RECONNECTING
	CLOSED
)

type Msg struct {
	Subject string
	Data    []byte
	Sub     *Subscription
}

func (nc *Conn) removeSub(sub *Subscription) error {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.status == CLOSED {
		return ErrConnectionClosed
	}
	sub.close()
	delete(nc.subs, sub.sid)
	return nil
}

func (nc *Conn) resubscribe() {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	for sid, sub := range nc.subs {
		sub.mu.Lock()
		if sub.closed {
			sub.mu.Unlock()
			delete(nc.subs, sid)
			continue
		}
		sub.mu.Unlock()
		// Resubscribe logic here...
	}
}
