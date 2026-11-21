# wschat

WebSocket pub/sub server (Go) with presence, fan-out, a typed TypeScript
client, and a browser demo, all in one binary.

## Run

    go run .

Open http://localhost:8080 in two tabs and chat. That's it: the web UI and the
compiled TypeScript client are embedded in the binary (`web/`), so no Node or
npm is needed to run.

Flags: `-addr :9000` to change the port.

## Load test (optional)

    ulimit -n 200000
    go run ./cmd/loadtest -n 10000 -rooms 50 -rate 0.2 -d 60s

Inspect: `curl localhost:8080/stats`, pprof at http://localhost:6060/debug/pprof/
Race check: `go run -race .`

## TypeScript client (only needed if you change it)

Source is in `client-ts/`. The compiled browser build is committed in `web/js/`.

    cd client-ts && npm install
    npm test                          # unit tests (fake WebSocket)
    npm run build:web                 # recompile demo + SDK into ../web/js
    npm run example -- alice general  # terminal chat client (Node 22+)

    import { ChatClient } from "wschat-client";
    const c = new ChatClient({ url: "ws://localhost:8080/ws", user: "alice" });
    c.subscribe<string>("general", (m) => console.log(m.from, m.data));
    c.onPresence((p) => console.log(p.from, p.data));
    await c.connect();
    c.publish("general", "hello");

## Layout

    main.go       server, routes, embedded web UI, graceful shutdown
    hub.go        sharded topic -> subscribers, fan-out
    client.go     per-connection read/write pumps, slow-consumer policy
    presence.go   multi-device presence with reconnect debounce
    web/          index.html + compiled JS (embedded via go:embed)
    client-ts/    TypeScript SDK, demo source, tests
    cmd/loadtest/ load generator
