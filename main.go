package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"io/fs"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// The browser demo (HTML + compiled TypeScript client) lives inside the binary,
// so `go run .` is the only command needed.
//
//go:embed web
var webFS embed.FS

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024, // small buffers = less memory per connection
	WriteBufferSize: 1024,
	// TODO: restrict origins in production.
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	hub := NewHub()
	pres := NewPresence(hub)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// TODO: replace with real auth (e.g. verify a JWT) before upgrading.
		user := r.URL.Query().Get("user")
		if user == "" {
			http.Error(w, "user required", http.StatusUnauthorized)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		NewClient(user, conn, hub, pres).Run()
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]int64{
			"active_connections":  activeConns.Load(),
			"messages_fanned":     messagesFanned.Load(),
			"slow_consumer_drops": droppedSlow.Load(),
		})
	})
	webRoot, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", http.FileServer(http.FS(webRoot)))

	srv := &http.Server{Addr: *addr, Handler: mux}

	// pprof on localhost only: http://localhost:6060/debug/pprof/
	go func() { log.Println(http.ListenAndServe("localhost:6060", nil)) }()

	go func() {
		host := *addr
		if strings.HasPrefix(host, ":") {
			host = "localhost" + host
		}
		log.Printf("listening on %s  ->  open http://%s", *addr, host)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	hub.CloseAll()
}
