package main

import (
	"hash/fnv"
	"sync"
)

const numShards = 64

type shard struct {
	mu     sync.RWMutex
	topics map[string]map[*Client]struct{}
}

// Hub maps topic -> subscribers. Sharded by topic hash so publishes and
// subscribes on different topics don't contend on one global lock.
type Hub struct {
	shards  [numShards]shard
	clients sync.Map // *Client -> struct{}, used for shutdown
}

func NewHub() *Hub {
	h := &Hub{}
	for i := range h.shards {
		h.shards[i].topics = make(map[string]map[*Client]struct{})
	}
	return h
}

func (h *Hub) shardFor(topic string) *shard {
	f := fnv.New32a()
	f.Write([]byte(topic))
	return &h.shards[f.Sum32()%numShards]
}

func (h *Hub) Subscribe(c *Client, topic string) {
	s := h.shardFor(topic)
	s.mu.Lock()
	subs, ok := s.topics[topic]
	if !ok {
		subs = make(map[*Client]struct{})
		s.topics[topic] = subs
	}
	subs[c] = struct{}{}
	s.mu.Unlock()
}

func (h *Hub) Unsubscribe(c *Client, topic string) {
	s := h.shardFor(topic)
	s.mu.Lock()
	if subs, ok := s.topics[topic]; ok {
		delete(subs, c)
		if len(subs) == 0 {
			delete(s.topics, topic) // don't leak empty topics
		}
	}
	s.mu.Unlock()
}

// Publish fans out one pre-serialized message. Subscribers are copied under
// the read lock; all I/O happens outside it.
func (h *Hub) Publish(topic string, msg []byte) {
	s := h.shardFor(topic)
	s.mu.RLock()
	subs := s.topics[topic]
	targets := make([]*Client, 0, len(subs))
	for c := range subs {
		targets = append(targets, c)
	}
	s.mu.RUnlock()

	for _, c := range targets {
		c.Enqueue(msg)
	}
}

func (h *Hub) CloseAll() {
	h.clients.Range(func(k, _ any) bool {
		k.(*Client).Close()
		return true
	})
}
