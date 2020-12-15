package dqueue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	capacity := 8
	dq := New(capacity)
	require.NotNil(t, dq)

	defer dq.Close()

	require.NotNil(t, dq.notifyC)
	require.NotNil(t, dq.mu)
	require.NotNil(t, dq.pq)
	require.Equal(t, dq.pq.Cap(), capacity)
	require.Equal(t, atomic.LoadInt32(&dq.sleeping), int32(0))
	require.NotNil(t, dq.wakeupC)
	require.NotNil(t, dq.exitC)
	require.NotNil(t, dq.wg)
	require.Equal(t, dq.state, int8(0))
}

func TestDefault(t *testing.T) {
	dq := Default()
	require.NotNil(t, dq)
	require.Equal(t, dq.pq.Cap(), defaultCapacity)
}

func TestDQueue_Close(t *testing.T) {
	dq := Default()
	dq.Close()
	require.Panics(t, func() {
		dq.Close()
	})
}

func TestDQueue_Consume(t *testing.T) {
	dq := Default()
	require.Equal(t, dq.state, int8(0))
	require.Equal(t, atomic.LoadInt32(&dq.sleeping), int32(0))

	// register
	dq.Consume(func(msg *Message) {})

	time.Sleep(time.Millisecond * 3)

	require.Equal(t, dq.state, int8(1))
	require.Equal(t, atomic.LoadInt32(&dq.sleeping), int32(1))

	// duplicate consume.
	require.Panics(t, func() {
		dq.Consume(func(msg *Message) {})
	})

	// consume a closed queue.
	dq.Close()
	require.Panics(t, func() {
		dq.Consume(func(msg *Message) {})
	})
}

func TestDQueue_offer(t *testing.T) {
	dq := Default()

	now := time.Now().Add(time.Second * 10).UnixNano()
	dq.offer(now, "1024")
	require.Equal(t, dq.Len(), 1)

	require.Equal(t, dq.pq.Peek().Key().(Int64), Int64(now))
	require.Equal(t, dq.pq.Peek().Value().(string), "1024")

	dq.mu.Lock()
	item := dq.pq.Pop()
	require.Equal(t, item.Key().(Int64), Int64(now))
	require.Equal(t, item.Value().(string), "1024")
	dq.mu.Unlock()
	require.Equal(t, dq.Len(), 0)
}

func TestDQueue_peekAndShift(t *testing.T) {
	dq := Default()

	// Add first item.
	exp1 := time.Now().Add(time.Millisecond * 50).UnixNano()
	dq.offer(exp1, "item1")
	require.Equal(t, dq.Len(), 1)

	// The first item not expires, get a delay time.
	dq.mu.Lock()
	msg, delay := dq.peekAndShift()
	require.Greater(t, delay, int64(0))
	require.Nil(t, msg)
	dq.mu.Unlock()
	require.Equal(t, dq.Len(), 1)

	// Add the second item.
	exp2 := time.Now().Add(time.Millisecond).UnixNano()
	dq.offer(exp2, "item2")
	require.Equal(t, dq.Len(), 2)

	// Waits for the second item expiration.
	time.Sleep(time.Millisecond * 2)

	// The second item is expires, get the item.
	dq.mu.Lock()
	msg, delay = dq.peekAndShift()
	require.Equal(t, delay, int64(0))
	require.NotNil(t, msg)
	require.Equal(t, msg.Expiration, exp2)
	require.Equal(t, msg.Value.(string), "item2")
	dq.mu.Unlock()
	require.Equal(t, dq.Len(), 1)

	// Waits for the first item expiration.
	time.Sleep(time.Millisecond * 50)

	// The first item is expires.
	dq.mu.Lock()
	msg, delay = dq.peekAndShift()
	require.Equal(t, delay, int64(0))
	require.NotNil(t, msg)
	require.Equal(t, msg.Expiration, exp1)
	require.Equal(t, msg.Value.(string), "item1")
	dq.mu.Unlock()
	require.Equal(t, dq.Len(), 0)
}

func TestDQueue_Len(t *testing.T) {
	dq := Default()

	require.Equal(t, dq.Len(), 0)

	seeds := []time.Duration{
		time.Millisecond * 1,
		time.Millisecond * 2,
		time.Millisecond * 3,
		time.Millisecond * 4,
		time.Millisecond * 5,
		time.Millisecond * 6,
		time.Millisecond * 7,
		time.Millisecond * 8,
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(seeds))

	for i := 0; i < len(seeds); i++ {
		dq.After(seeds[i], "1024")
		require.Equal(t, dq.Len(), i+1)
	}

	i := len(seeds) - 1
	dq.Consume(func(msg *Message) {
		require.Equal(t, dq.Len(), i)
		i--
		wg.Done()
	})

	go func() {
		// Waits for all task finished.
		wg.Wait()
		require.Equal(t, dq.Len(), 0)
		// Close the queue.
		dq.Close()
	}()

	// Wait for queue closed.
	dq.Wait()
}

type Result struct {
	T time.Time // Receive Time
	M *Message  // Receive Message.
}

func receiveAndCheck(t *testing.T, offer string) {
	dq := Default()
	defer dq.Close()

	checkC := make(chan *Result)

	dq.Consume(func(msg *Message) {
		checkC <- &Result{T: time.Now(), M: msg}
	})

	seeds := []time.Duration{
		time.Millisecond * 1,
		time.Millisecond * 5,
		time.Millisecond * 10,
		time.Millisecond * 50,
		time.Millisecond * 100,
		time.Millisecond * 400,
		time.Millisecond * 500,
		time.Second * 1,
	}

	lapse := time.Duration(0)
	start := time.Now()

	for _, d := range seeds {
		value := d.String()
		switch offer {
		case "after":
			dq.After(d, value)
		case "expire":
			dq.Expire(time.Now().Add(d).UnixNano(), value)
		}
	}

	for _, d := range seeds {
		lapse += d
		min := start.Add(d)
		max := start.Add(lapse + time.Millisecond*5)

		got := <-checkC

		// Check expiration and value
		require.GreaterOrEqual(t, got.M.Expiration, start.Add(d).UnixNano())
		require.Equal(t, d.String(), got.M.Value)

		// Check actual timestamp
		require.LessOrEqual(t, got.M.Actual, got.T.UnixNano())
		require.GreaterOrEqual(t, got.M.Actual, got.M.Expiration)

		// Check the receive timestamp.
		require.Greater(t, got.T.UnixNano(), min.UnixNano(), fmt.Sprintf("%s: got: %s, want: %s", d.String(), got.T.String(), min.String()))
		require.Less(t, got.T.UnixNano(), max.UnixNano(), fmt.Sprintf("%s: got: %s, want: %s", d.String(), got.T.String(), max.String()))
	}
}

func TestDQueue_After(t *testing.T) {
	receiveAndCheck(t, "after")
}

func TestDQueue_Expire(t *testing.T) {
	receiveAndCheck(t, "expire")
}
