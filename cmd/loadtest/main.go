// Load generator: opens N websocket connections spread over R rooms; each
// connection publishes at a fixed rate and counts what it receives.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var connected, failed, sent, received atomic.Int64

func main() {
	host := flag.String("host", "localhost:8080", "server host:port")
	n := flag.Int("n", 1000, "number of connections")
	rooms := flag.Int("rooms", 10, "number of rooms")
	rate := flag.Float64("rate", 0.2, "messages/sec per connection (0 = listen only)")
	ramp := flag.Duration("ramp", 2*time.Millisecond, "delay between dials")
	dur := flag.Duration("d", 60*time.Second, "test duration")
	flag.Parse()

	stop := make(chan struct{})
	for i := 0; i < *n; i++ {
		go runConn(i, *host, i%*rooms, *rate, stop)
		time.Sleep(*ramp)
	}

	start := time.Now()
	var lastSent, lastRecv int64
	for time.Since(start) < *dur {
		time.Sleep(time.Second)
		s, r := sent.Load(), received.Load()
		fmt.Printf("conns=%d failed=%d sent/s=%d recv/s=%d\n",
			connected.Load(), failed.Load(), s-lastSent, r-lastRecv)
		lastSent, lastRecv = s, r
	}
	close(stop)
	time.Sleep(500 * time.Millisecond)
}

func runConn(id int, host string, room int, rate float64, stop <-chan struct{}) {
	u := url.URL{Scheme: "ws", Host: host, Path: "/ws",
		RawQuery: fmt.Sprintf("user=load-%d", id)}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		failed.Add(1)
		if failed.Load() == 1 {
			log.Println("first dial error:", err)
		}
		return
	}
	defer c.Close()
	connected.Add(1)
	defer connected.Add(-1)

	topic := fmt.Sprintf("room-%d", room)
	c.WriteJSON(map[string]string{"type": "subscribe", "topic": topic})

	go func() { // reader
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
			received.Add(1)
		}
	}()

	if rate <= 0 {
		<-stop
		return
	}
	// jitter start so all clients don't fire in lockstep
	time.Sleep(time.Duration(id%1000) * time.Millisecond)
	t := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer t.Stop()
	for {
		select {
		case <-t.C:
			err := c.WriteJSON(map[string]any{"type": "publish", "topic": topic, "data": "ping"})
			if err != nil {
				return
			}
			sent.Add(1)
		case <-stop:
			return
		}
	}
}
