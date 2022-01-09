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

	defer dq.Stop()

	require.NotNil(t, dq.notifyC)
	require.NotNil(t, dq.mu)
	require.NotNil(t, dq.pq)
	require.Equal(t, dq.pq.Cap(), capacity)
	require.Equal(t, atomic.LoadInt32(&dq.sleeping), int32(0))
	require.NotNil(t, dq.wakeupC)
	require.NotNil(t, dq.exitC)
	require.NotNil(t, dq.wg)
	require.Equal(t, dq.state, int8(0))
	require.NotNil(t, dq.delayer)
}

func TestDefault(t *testing.T) {
	dq := Default()
	require.NotNil(t, dq)
	require.Equal(t, dq.pq.Cap(), defaultCapacity)
}

func TestDQueue_Close(t *testing.T) {
	dq := Default()
	dq.Stop()
	dq.Stop()
	//require.Panics(t, func() {
	//	dq.Stop()
	//})
}

func TestDQueue_Start(t *testing.T) {
	dq := Default()
	require.Equal(t, dq.state, int8(0))
	require.Equal(t, atomic.LoadInt32(&dq.sleeping), int32(0))

	// register
	dq.Start(func(val Value) {})

	time.Sleep(time.Millisecond * 3)

	require.Equal(t, dq.state, int8(1))
	require.Equal(t, atomic.LoadInt32(&dq.sleeping), int32(1))

	// duplicate consume.
	require.Panics(t, func() {
		dq.Start(func(val Value) {})
	})

	// consume a closed queue.
	dq.Stop()
	require.Panics(t, func() {
		dq.Start(func(val Value) {})
	})
}

func TestDQueue_offer(t *testing.T) {
	dq := Default()

	now := time.Now().Add(time.Second * 10).UnixNano()
	dq.Offer(now, "1024")
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
	dq.Offer(exp1, "item1")
	require.Equal(t, dq.Len(), 1)

	// The first item not expires, get a delay time.
	dq.mu.Lock()
	val, delay := dq.peekAndShift()
	require.Greater(t, int64(delay), int64(0))
	require.Nil(t, val)
	dq.mu.Unlock()
	require.Equal(t, dq.Len(), 1)

	// Add the second item.
	exp2 := time.Now().Add(time.Millisecond).UnixNano()
	dq.Offer(exp2, "item2")
	require.Equal(t, dq.Len(), 2)

	// Waits for the second item expiration.
	time.Sleep(time.Millisecond * 2)

	// The second item is expires, get the item.
	dq.mu.Lock()
	val, delay = dq.peekAndShift()
	require.Equal(t, int64(delay), int64(0))
	require.NotNil(t, val)
	//require.Equal(t, msg.Expiration, exp2)
	require.Equal(t, val.(string), "item2")
	dq.mu.Unlock()
	require.Equal(t, dq.Len(), 1)

	// Waits for the first item expiration.
	time.Sleep(time.Millisecond * 50)

	// The first item is expires.
	dq.mu.Lock()
	val, delay = dq.peekAndShift()
	require.Equal(t, int64(delay), int64(0))
	require.NotNil(t, val)
	//require.Equal(t, msg.Expiration, exp1)
	require.Equal(t, val.(string), "item1")
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
		x := time.Now().Add(seeds[i]).UnixNano()
		dq.Offer(x, "1024")
		require.Equal(t, dq.Len(), i+1)
	}

	i := len(seeds) - 1
	dq.Start(func(val Value) {
		require.Equal(t, dq.Len(), i)
		i--
		wg.Done()
	})

	// Waits for all task finished.
	wg.Wait()

	require.Equal(t, dq.Len(), 0)
	// Stop the queue.
	dq.Stop()
}

type Result struct {
	T time.Time // Receive Time
	V Value     // Receive Message.
}

func TestDQueue_Offer(t *testing.T) {
	dq := Default()
	defer dq.Stop()

	checkC := make(chan *Result)

	dq.Start(func(val Value) {
		checkC <- &Result{T: time.Now(), V: val}
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
		dq.Offer(time.Now().Add(d).UnixNano(), value)
	}

	for _, d := range seeds {
		lapse += d
		min := start.Add(d)
		max := start.Add(lapse + time.Millisecond*5)

		got := <-checkC

		// Check expiration and value
		require.Equal(t, d.String(), got.V)

		// Check the receive timestamp.
		require.Greater(t, got.T.UnixNano(), min.UnixNano(), fmt.Sprintf("%s: got: %s, want: %s", d.String(), got.T.String(), min.String()))
		require.Less(t, got.T.UnixNano(), max.UnixNano(), fmt.Sprintf("%s: got: %s, want: %s", d.String(), got.T.String(), max.String()))
	}
}
