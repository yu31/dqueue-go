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
type (
	Int64 = container.Int64
	Value = container.Value
)

// Delayer to calculate the delay time by expiration.
type Delayer func(expiration int64) (delay time.Duration)

func defaultDelayer(expiration int64) (delay time.Duration) {
	return time.Duration(expiration - time.Now().UnixNano())
}

// Consumer is a function for consume the expires data.
type Consumer func(msg Value)

// DQueue implements a delay queue with concurrency safe base on priority queue (min heap).
type DQueue struct {
	notifyC chan Value // Notify channel.

	pq *minheap.MinHeap // A priority queue implemented with min heap.
	mu *sync.Mutex      // Mutex lock for any operations with pq.

	sleeping int32         // Similar to the sleeping state of runtime.timers. 1 => true, 0 => false.
	wakeupC  chan struct{} // The wakeupC for wakeup the polling goroutine if new item add to queue head.

	exitC chan struct{}   // The exitC is used to notify the polling and receive stop.
	wg    *sync.WaitGroup // The wg is used to waits the polling and receive finish.

	state int8 // The queue states, 0 for initialing, 1 for started and 2 for stopped.

	delayer Delayer
}

// Default creates an DQueue with default parameters.
func Default() *DQueue {
	return New(defaultCapacity)
}

// New creates an DQueue with given initialization queue capacity c.
// And execute polling in a goroutine.
func New(c int) *DQueue {
	return &DQueue{
		notifyC:  make(chan Value),
		pq:       minheap.New(c),
		mu:       new(sync.Mutex),
		sleeping: 0,
		wakeupC:  make(chan struct{}),
		exitC:    make(chan struct{}),
		wg:       new(sync.WaitGroup),
		state:    0,
		delayer:  defaultDelayer,
	}
}

// WithDelayer for reset the delayer func
func (dq *DQueue) WithDelayer(delayer Delayer) *DQueue {
	dq.delayer = delayer
	return dq
}

// Len returns the number of not expired data in the queue.
func (dq *DQueue) Len() int {
	dq.mu.Lock()
	n := dq.pq.Len()
	dq.mu.Unlock()
	return n
}

// Start register a func in its own goroutine to consume the expiration data.
// Only one consumer is allowed.
//
// You can calls the Wait method to blocks the main process after.
func (dq *DQueue) Start(f Consumer) {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	// Has been started.
	if dq.state == 1 {
		panic("dqueue: consume of consuming queue")
	}
	// The queue has been stopped.
	if dq.state == 2 {
		panic("dqueue: consume of closed queue")
	}
	// Set the states to started.
	dq.state = 1

	// Async polling in goroutine.
	dq.wg.Add(1)
	go func() {
		dq.polling()
		dq.wg.Done()
	}()
	// Async consuming in goroutine.
	dq.wg.Add(1)
	go func() {
		dq.consuming(f)
		dq.wg.Done()
	}()
}

// Stop for stop the delay queue.
func (dq *DQueue) Stop() {
	dq.mu.Lock()

	// The queue has been stopped.
	if dq.state == 2 {
		dq.mu.Unlock()
		return
	}

	// Set the states to closed.
	dq.state = 2
	// Stop the exit channel to notifies.
	close(dq.exitC)

	dq.mu.Unlock()

	// waits the running goroutine exit.
	dq.wg.Wait()
}

// Offer adds the value of the specified expiration(default is nano seconds timestamp) to the queue.
// e.g. dq.Offer(time.Now().Add(time.Second).UnixNano(), "1024")
func (dq *DQueue) Offer(expiration int64, value Value) {
	dq.mu.Lock()
	item := dq.pq.Push(Int64(expiration), value)
	index := item.Index()
	dq.mu.Unlock()

	// A new item with the earliest expiration is added.
	if index == 0 && atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
		dq.wakeupC <- struct{}{}
	}
}

func (dq *DQueue) consuming(f Consumer) {
	for {
		select {
		case <-dq.exitC:
			return
		case val := <-dq.notifyC:
			f(val)
		}
	}
}

func (dq *DQueue) peekAndShift() (Value, time.Duration) {
	item := dq.pq.Peek()
	if item == nil {
		// The queue is empty.
		return nil, 0
	}

	expiration := int64(item.Key().(Int64))

	delay := dq.delayer(expiration)
	if delay > 0 {
		return nil, delay
	}
	// Removes item from queue top.
	_ = dq.pq.Pop()
	return item.Value(), 0
}

func (dq *DQueue) polling() {
LOOP:
	for {
		dq.mu.Lock()
		val, delay := dq.peekAndShift()
		if val == nil {
			// No items left or at least one item is pending.

			// We must ensure the atomicity of the whole operation, which is
			// composed of the above PeekAndShift and the following StoreInt32,
			// to avoid possible race conditions between Offer and Poll.
			atomic.StoreInt32(&dq.sleeping, 1)
		}
		dq.mu.Unlock()

		// No items in queue. Waiting to be wakeup.
		if val == nil && delay == 0 {
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
			case <-time.After(delay):
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
		case dq.notifyC <- val:
			// The expired data has been sent out successfully.
		}
	}

	// Reset the sleeping states before exits.
	if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
		// A caller of offer() is may being blocked on sending to wakeupC,
		// drain wakeupC to unblock the caller.
		select {
		case <-dq.wakeupC:
		default:
		}
	}
}
