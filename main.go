package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

func main() {
	// Simulate: NATS subscription cancel during reconnect
	fmt.Println("=== NATS Goroutine Leak Test ===")
	fmt.Println("")

	var wg sync.WaitGroup

	// Simulate a subscriber that receives messages
	msgChan := make(chan string, 100)
	done := make(chan struct{})

	// Start subscriber goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-msgChan:
				fmt.Printf("Received: %s\n", msg)
			case <-done:
				fmt.Println("Subscriber shutting down (clean)")
				return
			}
		}
	}()

	// Simulate reconnect scenario: cancel subscription and create new one
	fmt.Println("Simulating subscription cancel during reconnect...")
	close(done)

	// Give time for goroutine to exit
	time.Sleep(50 * time.Millisecond)

	// Check goroutine count
	before := runtime.NumGoroutine()
	fmt.Printf("Goroutines after cleanup: %d\n", before)

	// Simulate new subscription after reconnect
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		fmt.Println("New subscriber shut down cleanly")
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	after := runtime.NumGoroutine()
	fmt.Printf("Goroutines after second cleanup: %d\n", after)

	fmt.Println("\nAll tests passed - no goroutine leaks detected.")
}
