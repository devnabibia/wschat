package main

import "encoding/json"

// Client -> server
//
//	{"type":"subscribe","topic":"room1"}
//	{"type":"unsubscribe","topic":"room1"}
//	{"type":"publish","topic":"room1","data":"hello"}
//	{"type":"who"}                      -> list of online users
type Inbound struct {
	Type  string          `json:"type"`
	Topic string          `json:"topic,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
}

// Server -> client
//
//	type: "message" | "presence" | "who" | "error" | "ok"
type Outbound struct {
	Type  string          `json:"type"`
	Topic string          `json:"topic,omitempty"`
	From  string          `json:"from,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	TS    int64           `json:"ts,omitempty"`
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
