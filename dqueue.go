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

// Receiver is a function for receive expires data.
type Receiver func(msg *Message)

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
// Inspired by https://github.com/RussellLuo/timingwheel/blob/master/delayqueue/delayqueue.go
type DQueue struct {
	notifyC chan *Message // Notify channel.

	mu *sync.Mutex
	pq *minheap.MinHeap // A priority queue implemented with min heap.

	sleeping int32         // Similar to the sleeping state of runtime.timers. 1 => true, 0 => false.
	wakeupC  chan struct{} // The wakeupC for wakeup the polling goroutine if new item add to queue head.

	exitC chan struct{}   // The exitC is used to notify the polling and receive stop.
	wg    *sync.WaitGroup // The wg is used to waits the polling and receive finish.
	ready int32           // The ready represents whether the polling is ready. 1 => true, 0 => false.
}

// Default creates an DQueue with default parameters.
func Default() *DQueue {
	return New(defaultCapacity)
}

// New creates an DQueue with given initialization queue capacity c.
// And execute polling in a goroutine.
func New(c int) *DQueue {
	dq := newDQueue(c)
	go dq.polling()
	return dq
}

// newDQueue is an internal helper function that really creates an DQueue.
func newDQueue(c int) *DQueue {
	return &DQueue{
		notifyC:  make(chan *Message),
		pq:       minheap.New(c),
		mu:       new(sync.Mutex),
		sleeping: 0,
		wakeupC:  make(chan struct{}),
		exitC:    make(chan struct{}),
		wg:       new(sync.WaitGroup),
		ready:    0,
	}
}

// Len return the number of elements in the queue.
func (dq *DQueue) Len() int {
	dq.mu.Lock()
	n := dq.pq.Len()
	dq.mu.Unlock()
	return n
}

// Close for close the delay queue.
// The func can't be called repeatedly.
func (dq *DQueue) Close() {
	// Waiting for the polling startup to prevents `DATA RACE` waring.
	// There may be data race between wg.Wait and wg.Add if they are called at the same time.
	for atomic.LoadInt32(&dq.ready) == 0 {
		time.Sleep(time.Millisecond * 3)
	}

	close(dq.exitC)
	// Waiting for other goroutine exits.
	dq.wg.Wait()
}

// Receive register a func to receive expiration data.
func (dq *DQueue) Receive(f Receiver) {
	dq.wg.Add(1)
	defer dq.wg.Done()

	for {
		select {
		case <-dq.exitC:
			return
		case msg := <-dq.notifyC:
			f(msg)
		}
	}
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
	dq.wg.Add(1)
	defer func() {
		// Reset the sleeping states.
		atomic.StoreInt32(&dq.sleeping, 0)
		dq.wg.Done()
	}()

	atomic.StoreInt32(&dq.ready, 1)

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
}
