package main

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	sendBuffer     = 256
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 4096
	maxTopics      = 100
)

var (
	activeConns    atomic.Int64
	droppedSlow    atomic.Int64
	messagesFanned atomic.Int64
)

type Client struct {
	user string
	conn *websocket.Conn
	hub  *Hub
	pres *Presence

	send chan []byte   // never closed; writePump exits via done
	done chan struct{} // closed once on Close()
	once sync.Once

	mu     sync.Mutex
	topics map[string]struct{}
}

func NewClient(user string, conn *websocket.Conn, hub *Hub, pres *Presence) *Client {
	return &Client{
		user:   user,
		conn:   conn,
		hub:    hub,
		pres:   pres,
		send:   make(chan []byte, sendBuffer),
		done:   make(chan struct{}),
		topics: make(map[string]struct{}),
	}
}

// Enqueue is non-blocking. A client that can't keep up is disconnected
// rather than allowed to stall the publisher (slow-consumer policy).
func (c *Client) Enqueue(msg []byte) {
	select {
	case c.send <- msg:
		messagesFanned.Add(1)
	default:
		droppedSlow.Add(1)
		log.Printf("slow consumer %s: disconnecting", c.user)
		c.Close()
	}
}

func (c *Client) Close() {
	c.once.Do(func() {
		close(c.done)
		c.conn.Close()
	})
}

func (c *Client) Run() {
	activeConns.Add(1)
	c.hub.clients.Store(c, struct{}{})
	c.pres.Join(c.user)

	go c.writePump()
	c.readPump() // blocks until the connection dies

	// cleanup
	c.mu.Lock()
	for t := range c.topics {
		c.hub.Unsubscribe(c, t)
	}
	c.topics = nil
	c.mu.Unlock()
	c.hub.clients.Delete(c)
	c.pres.Leave(c.user)
	c.Close()
	activeConns.Add(-1)
}

func (c *Client) readPump() {
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		// any inbound traffic also proves liveness
		c.conn.SetReadDeadline(time.Now().Add(pongWait))

		var in Inbound
		if err := json.Unmarshal(raw, &in); err != nil {
			c.reply("error", "", "bad json")
			continue
		}
		c.handle(in)
	}
}

func (c *Client) handle(in Inbound) {
	switch in.Type {
	case "subscribe":
		if in.Topic == "" {
			c.reply("error", "", "topic required")
			return
		}
		c.mu.Lock()
		if c.topics == nil || len(c.topics) >= maxTopics {
			c.mu.Unlock()
			c.reply("error", in.Topic, "too many topics")
			return
		}
		_, already := c.topics[in.Topic]
		c.topics[in.Topic] = struct{}{}
		c.mu.Unlock()
		if !already {
			c.hub.Subscribe(c, in.Topic)
		}
		c.reply("ok", in.Topic, "subscribed")

	case "unsubscribe":
		c.mu.Lock()
		_, had := c.topics[in.Topic]
		delete(c.topics, in.Topic)
		c.mu.Unlock()
		if had {
			c.hub.Unsubscribe(c, in.Topic)
		}
		c.reply("ok", in.Topic, "unsubscribed")

	case "publish":
		c.mu.Lock()
		_, ok := c.topics[in.Topic]
		c.mu.Unlock()
		if !ok {
			c.reply("error", in.Topic, "subscribe first")
			return
		}
		// Serialize ONCE, share the same []byte with every subscriber.
		msg := mustMarshal(Outbound{
			Type: "message", Topic: in.Topic, From: c.user,
			Data: in.Data, TS: time.Now().UnixMilli(),
		})
		c.hub.Publish(in.Topic, msg)

	case "who":
		c.Enqueue(mustMarshal(Outbound{
			Type: "who", Data: mustMarshal(c.pres.Online()),
		}))

	default:
		c.reply("error", "", "unknown type")
	}
}

func (c *Client) reply(typ, topic, text string) {
	c.Enqueue(mustMarshal(Outbound{Type: typ, Topic: topic, Data: mustMarshal(text)}))
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	defer c.Close()

	for {
		select {
		case msg := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}
