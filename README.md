# NATS Go Goroutine Leak Fix

## Fix: Goroutine leak when subscription is cancelled during automatic reconnect

### Problem
When a NATS subscription is cancelled while the client is in the middle of
an automatic reconnection, subscriber goroutines can leak because the cleanup
path doesn't account for the reconnect state.

### Fix
Ensures subscription cleanup properly terminates goroutines even when the
connection is in a reconnecting state.

### Test
```bash
go run main.go
```
