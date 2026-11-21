import { test } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { ChatClient } from "../src/client.js";

class FakeWS {
  static instances: FakeWS[] = [];
  readyState = 0;
  sent: string[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((e: { data: unknown }) => void) | null = null;
  onclose: ((e: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  constructor(public url: string) {
    FakeWS.instances.push(this);
  }
  send(s: string) { this.sent.push(s); }
  close() { this.readyState = 3; this.onclose?.({ code: 1000, reason: "" }); }
  // test helpers
  open() { this.readyState = 1; this.onopen?.(); }
  receive(o: unknown) { this.onmessage?.({ data: JSON.stringify(o) }); }
  drop() { this.readyState = 3; this.onclose?.({ code: 1006, reason: "lost" }); }
}

function make() {
  FakeWS.instances = [];
  return new ChatClient({
    url: "ws://x/ws",
    user: "bob",
    WebSocketImpl: FakeWS as unknown as typeof WebSocket,
    baseDelayMs: 1,
    maxDelayMs: 2,
  });
}

test("subscribes on open, dispatches messages, resubscribes after reconnect", async () => {
  const c = make();
  const got: unknown[] = [];
  c.subscribe("room", (m) => got.push(m.data)); // before connect: replayed on open

  const p = c.connect();
  const ws1 = FakeWS.instances[0]!;
  assert.equal(ws1.url, "ws://x/ws?user=bob");
  ws1.open();
  await p;
  assert.deepEqual(JSON.parse(ws1.sent[0]!), { type: "subscribe", topic: "room" });

  ws1.receive({ type: "message", topic: "room", from: "a", data: "hi", ts: 1 });
  assert.deepEqual(got, ["hi"]);

  ws1.drop(); // network failure
  await sleep(50);
  const ws2 = FakeWS.instances[1]!;
  assert.ok(ws2, "should have reconnected");
  ws2.open();
  assert.deepEqual(JSON.parse(ws2.sent[0]!), { type: "subscribe", topic: "room" });
  c.close();
});

test("who() resolves with the server's reply", async () => {
  const c = make();
  const p = c.connect();
  const ws = FakeWS.instances[0]!;
  ws.open();
  await p;
  const w = c.who();
  assert.deepEqual(JSON.parse(ws.sent.at(-1)!), { type: "who" });
  ws.receive({ type: "who", data: ["alice", "bob"] });
  assert.deepEqual(await w, ["alice", "bob"]);
  c.close();
});

test("unsubscribe sends only when last handler is removed; publish offline returns false", async () => {
  const c = make();
  assert.equal(c.publish("room", "x"), false);
  const p = c.connect();
  const ws = FakeWS.instances[0]!;
  ws.open();
  await p;
  const off1 = c.subscribe("t", () => {});
  const off2 = c.subscribe("t", () => {});
  off1();
  assert.equal(ws.sent.filter((s) => s.includes("unsubscribe")).length, 0);
  off2();
  assert.equal(ws.sent.filter((s) => s.includes("unsubscribe")).length, 1);
  c.close();
});
