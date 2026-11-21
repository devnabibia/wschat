package main

import (
	"sort"
	"sync"
	"time"
)

const (
	presenceTopic = "presence"
	presenceGrace = 3 * time.Second // debounce reconnects / page refreshes
)

// Presence tracks a connection COUNT per user (multi-tab / multi-device).
// A user is "offline" only after their last connection has been gone for
// presenceGrace, so quick reconnects don't flap join/leave events.
type Presence struct {
	mu     sync.Mutex
	counts map[string]int
	timers map[string]*time.Timer
	hub    *Hub
}

func NewPresence(h *Hub) *Presence {
	return &Presence{
		counts: make(map[string]int),
		timers: make(map[string]*time.Timer),
		hub:    h,
	}
}

func (p *Presence) Join(user string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if t, ok := p.timers[user]; ok { // reconnected within grace period
		t.Stop()
		delete(p.timers, user)
		p.counts[user]++
		return
	}
	p.counts[user]++
	if p.counts[user] == 1 {
		p.broadcast(user, "online")
	}
}

func (p *Presence) Leave(user string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.counts[user]--
	if p.counts[user] > 0 {
		return
	}
	var t *time.Timer
	t = time.AfterFunc(presenceGrace, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.timers[user] != t { // cancelled by a reconnect
			return
		}
		delete(p.timers, user)
		delete(p.counts, user)
		p.broadcast(user, "offline")
	})
	p.timers[user] = t
}

func (p *Presence) Online() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.counts))
	for u, n := range p.counts {
		if n > 0 {
			out = append(out, u)
		}
	}
	sort.Strings(out)
	return out
}

// caller holds p.mu; Publish only does non-blocking sends, so this is safe.
func (p *Presence) broadcast(user, status string) {
	p.hub.Publish(presenceTopic, mustMarshal(Outbound{
		Type: "presence", Topic: presenceTopic, From: user,
		Data: mustMarshal(status), TS: time.Now().UnixMilli(),
	}))
}
