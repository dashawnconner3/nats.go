package nats

import (
	"sync"
	"time"
)

// Subscription represents a interest in a given subject.
type Subscription struct {
	mu        sync.Mutex
	sid       int64
	subject   string
	queue     string
	mch       chan *Msg
	conn      *Conn
	closed    bool
	pCond     *sync.Cond
	max       int64
	delivered int64
}

func (sub *Subscription) Unsubscribe() error {
	sub.mu.Lock()
	conn := sub.conn
	closed := sub.closed
	sub.mu.Unlock()
	if conn == nil || closed {
		return ErrBadSubscription
	}
	return conn.removeSub(sub)
}

func (sub *Subscription) close() {
	sub.mu.Lock()
	if sub.closed {
		sub.mu.Unlock()
		return
	}
	sub.closed = true
	if sub.mch != nil {
		close(sub.mch)
		sub.mch = nil
	}
	if sub.pCond != nil {
		sub.pCond.Broadcast()
	}
	sub.mu.Unlock()
}
