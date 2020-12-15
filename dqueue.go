// Copyright (c) 2020, Yu Wu <yu.771991@gmail.com> All rights reserved.
//
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package dqueue

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/yu31/gostructs/container"
	"github.com/yu31/gostructs/minheap"
)

const (
	defaultCapacity = 64
)

// Type aliases for simplifying use in this package.
type Int64 = container.Int64
type Value = container.Value

// Consumer is a function for consume the expires data.
type Consumer func(msg *Message)

// Message is a message that sends expiration data to the Receiver.
type Message struct {
	// Actual is the timestamp that when generates the message.
	Actual int64
	// Expiration is the expiration timestamp of the Value.
	Expiration int64
	// Value is the value that added by After or Expire.
	Value Value
}

// DQueue implements a delay queue with concurrent safe base on priority queue (min heap).
type DQueue struct {
	notifyC chan *Message // Notify channel.

	mu *sync.Mutex
	pq *minheap.MinHeap // A priority queue implemented with min heap.

	sleeping int32         // Similar to the sleeping state of runtime.timers. 1 => true, 0 => false.
	wakeupC  chan struct{} // The wakeupC for wakeup the polling goroutine if new item add to queue head.

	exitC chan struct{}   // The exitC is used to notify the polling and receive stop.
	wg    *sync.WaitGroup // The wg is used to waits the polling and receive finish.

	state int8 // The queue states, 0 for initialing, 1 for consuming and 2 for closed.
}

// Default creates an DQueue with default parameters.
func Default() *DQueue {
	return New(defaultCapacity)
}

// New creates an DQueue with given initialization queue capacity c.
// And execute polling in a goroutine.
func New(c int) *DQueue {
	return &DQueue{
		notifyC:  make(chan *Message),
		pq:       minheap.New(c),
		mu:       new(sync.Mutex),
		sleeping: 0,
		wakeupC:  make(chan struct{}),
		exitC:    make(chan struct{}),
		wg:       new(sync.WaitGroup),
		state:    0,
	}
}

// Len returns the number of not expired data in the queue.
func (dq *DQueue) Len() int {
	dq.mu.Lock()
	n := dq.pq.Len()
	dq.mu.Unlock()
	return n
}

// Consume register a func in its own goroutine to consume the expiration data.
// Only one consumer is allowed.
//
// You can calls the Wait method to blocks the main process after.
func (dq *DQueue) Consume(f Consumer) {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	// Already in consuming.
	if dq.state == 1 {
		panic("dqueue: already in consuming")
	}
	// The queue has been closed.
	if dq.state == 2 {
		panic("dqueue: consume of closed queue")
	}
	// Set the states to consuming.
	dq.state = 1

	// Async do polling and consume.
	dq.wg.Add(2)
	go func() {
		dq.polling()
		dq.wg.Done()
	}()
	go func() {
		dq.consume(f)
		dq.wg.Done()
	}()
}

// Wait for blocking until queue closed.
// You should calls it after Consume.
func (dq *DQueue) Wait() {
	dq.wg.Wait()
}

// Close for close the queue.
func (dq *DQueue) Close() {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	// The queue has been closed.
	if dq.state == 2 {
		panic("dquque: close of closed queue")
	}
	// Set the states to closed.
	dq.state = 2

	// Close the exit channel to notifies and waits the running goroutine exit.
	close(dq.exitC)
	dq.wg.Wait()
}

// After adds the value of the specified delay time to the queue.
// e.g. dq.After(time.Millisecond, "1024")
func (dq *DQueue) After(delay time.Duration, value Value) {
	dq.offer(time.Now().Add(delay).UnixNano(), value)
}

// Expire adds the value of the specified expiration timestamp to the queue.
// e.g. dq.Expire(time.Now().Add(time.Second).UnixNano(), "1024")
func (dq *DQueue) Expire(expiration int64, value Value) {
	dq.offer(expiration, value)
}

func (dq *DQueue) offer(expiration int64, value Value) {
	dq.mu.Lock()
	item := dq.pq.Push(Int64(expiration), value)
	index := item.Index()
	dq.mu.Unlock()

	// A new item with the earliest expiration is added.
	if index == 0 && atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
		dq.wakeupC <- struct{}{}
	}
}

func (dq *DQueue) consume(f Consumer) {
	for {
		select {
		case <-dq.exitC:
			return
		case msg := <-dq.notifyC:
			f(msg)
		}
	}
}

func (dq *DQueue) peekAndShift() (*Message, int64) {
	item := dq.pq.Peek()
	if item == nil {
		// The queue is empty.
		return nil, 0
	}

	expiration := int64(item.Key().(Int64))
	now := time.Now().UnixNano()
	delay := expiration - now
	if delay > 0 {
		return nil, delay
	}

	// Removes item from queue top.
	_ = dq.pq.Pop()

	msg := &Message{
		Actual:     now,
		Expiration: expiration,
		Value:      item.Value(),
	}
	return msg, 0
}

func (dq *DQueue) polling() {
LOOP:
	for {
		dq.mu.Lock()
		msg, delay := dq.peekAndShift()
		if msg == nil {
			// No items left or at least one item is pending.

			// We must ensure the atomicity of the whole operation, which is
			// composed of the above PeekAndShift and the following StoreInt32,
			// to avoid possible race conditions between Offer and Poll.
			atomic.StoreInt32(&dq.sleeping, 1)
		}
		dq.mu.Unlock()

		// No items in queue. Waiting to be wakeup.
		if msg == nil && delay == 0 {
			select {
			case <-dq.exitC:
				break LOOP
			case <-dq.wakeupC:
			}

			continue LOOP
		}

		// At least one item is pending. Go to sleep.
		if delay > 0 {
			select {
			case <-dq.exitC:
				break LOOP
			case <-dq.wakeupC:
				// A new item with an "earlier" expiration than the current "earliest" one is added.
			case <-time.After(time.Duration(delay)):
				// The current "earliest" item expires.

				// Reset the sleeping state since there's no need to receive from wakeupC.
				if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
					// A caller of offer() is being blocked on sending to wakeupC,
					// drain wakeupC to unblock the caller.
					<-dq.wakeupC
				}
			}

			continue LOOP
		}

		// Send expired data to channel.
		select {
		case <-dq.exitC:
			break LOOP
		case dq.notifyC <- msg:
			// The expired data has been sent out successfully.
		}
	}

	// Reset the sleeping states before exits.
	atomic.StoreInt32(&dq.sleeping, 0)
}
