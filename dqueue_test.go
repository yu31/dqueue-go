package dqueue

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	capacity := 8
	dq := newDQueue(capacity)
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
	require.Equal(t, atomic.LoadInt32(&dq.ready), int32(0))

	go dq.polling()
	time.Sleep(time.Millisecond * 3)

	require.Equal(t, atomic.LoadInt32(&dq.sleeping), int32(1))
	require.Equal(t, atomic.LoadInt32(&dq.ready), int32(1))
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

func TestDQueue_offer(t *testing.T) {
	dq := newDQueue(16)

	now := time.Now().Add(time.Second * 10).UnixNano()
	dq.offer(now, "1024")
	require.Equal(t, dq.pq.Len(), 1)

	require.Equal(t, dq.pq.Peek().Key().(Int64), Int64(now))
	require.Equal(t, dq.pq.Peek().Value().(string), "1024")

	dq.mu.Lock()
	item := dq.pq.Pop()
	require.Equal(t, item.Key().(Int64), Int64(now))
	require.Equal(t, item.Value().(string), "1024")
	dq.mu.Unlock()
}

func TestDQueue_peekAndShift(t *testing.T) {
	dq := newDQueue(16)

	exp := time.Now().Add(time.Second * 10).UnixNano()
	dq.offer(exp, "1024")

	dq.mu.Lock()
	msg, delay := dq.peekAndShift()
	require.Greater(t, delay, int64(0))
	require.Nil(t, msg)
	dq.mu.Unlock()

	exp = time.Now().Add(time.Millisecond).UnixNano()
	dq.offer(exp, "1024")

	time.Sleep(time.Millisecond * 2)

	dq.mu.Lock()
	msg, delay = dq.peekAndShift()
	require.Equal(t, delay, int64(0))
	require.NotNil(t, msg)
	require.Equal(t, msg.Expiration, exp)
	require.Equal(t, msg.Value.(string), "1024")
	dq.mu.Unlock()
}

type Result struct {
	T time.Time // Receive Time
	M *Message  // Receive Message.
}

func receiveAndCheck(t *testing.T, offer string) {
	dq := Default()
	defer dq.Close()

	checkC := make(chan *Result)

	go func() {
		dq.Receive(func(msg *Message) {
			checkC <- &Result{T: time.Now(), M: msg}
		})
	}()

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
