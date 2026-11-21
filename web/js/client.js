import { PRESENCE_TOPIC, } from "./protocol.js";
const OPEN = 1; // WebSocket.OPEN
export class ChatClient {
    opts;
    ws = null;
    attempt = 0;
    closedByUser = false;
    reconnectTimer;
    // topic -> handlers. Its keys are replayed to the server after every reconnect.
    subs = new Map();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    listeners = new Map();
    whoWaiters = [];
    constructor(opts) {
        this.opts = {
            reconnect: true,
            baseDelayMs: 500,
            maxDelayMs: 15_000,
            ...opts,
        };
    }
    get connected() {
        return this.ws?.readyState === OPEN;
    }
    /**
     * Resolves on the first successful open. With reconnect enabled, it keeps
     * retrying in the background until the server is reachable.
     */
    connect() {
        this.closedByUser = false;
        return new Promise((resolve, reject) => {
            const offOpen = this.on("open", () => {
                offOpen();
                offClose();
                resolve();
            });
            const offClose = this.on("close", () => {
                if (!this.opts.reconnect) {
                    offOpen();
                    offClose();
                    reject(new Error("connection failed"));
                }
            });
            this.open();
        });
    }
    close() {
        this.closedByUser = true;
        clearTimeout(this.reconnectTimer);
        this.ws?.close();
    }
    /** Subscribe to a topic. Returns an unsubscribe function. */
    subscribe(topic, handler) {
        let set = this.subs.get(topic);
        if (!set) {
            set = new Set();
            this.subs.set(topic, set);
            this.send({ type: "subscribe", topic }); // no-op if offline; replayed on open
        }
        set.add(handler);
        return () => {
            const s = this.subs.get(topic);
            if (!s)
                return;
            s.delete(handler);
            if (s.size === 0) {
                this.subs.delete(topic);
                this.send({ type: "unsubscribe", topic });
            }
        };
    }
    /** Publish JSON-serializable data. Returns false if not currently connected. */
    publish(topic, data) {
        return this.send({ type: "publish", topic, data });
    }
    /** Be notified when users go online/offline. Returns a stop function. */
    onPresence(fn) {
        const off = this.on("presence", fn);
        const unsub = this.subscribe(PRESENCE_TOPIC, () => { });
        return () => {
            off();
            unsub();
        };
    }
    /** Ask the server who is online right now. */
    who(timeoutMs = 5000) {
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.whoWaiters = this.whoWaiters.filter((w) => w !== waiter);
                reject(new Error("who() timed out"));
            }, timeoutMs);
            const waiter = (users) => {
                clearTimeout(timer);
                resolve(users);
            };
            this.whoWaiters.push(waiter);
            if (!this.send({ type: "who" })) {
                clearTimeout(timer);
                this.whoWaiters = this.whoWaiters.filter((w) => w !== waiter);
                reject(new Error("not connected"));
            }
        });
    }
    on(event, fn) {
        let set = this.listeners.get(event);
        if (!set) {
            set = new Set();
            this.listeners.set(event, set);
        }
        const s = set;
        s.add(fn);
        return () => s.delete(fn);
    }
    // ---- internals ----
    emit(event, ...args) {
        this.listeners.get(event)?.forEach((fn) => fn(...args));
    }
    send(msg) {
        if (!this.ws || this.ws.readyState !== OPEN)
            return false;
        this.ws.send(JSON.stringify(msg));
        return true;
    }
    open() {
        const Impl = this.opts.WebSocketImpl ?? WebSocket;
        const ws = new Impl(`${this.opts.url}?user=${encodeURIComponent(this.opts.user)}`);
        this.ws = ws;
        ws.onopen = () => {
            this.attempt = 0;
            for (const topic of this.subs.keys())
                this.send({ type: "subscribe", topic });
            this.emit("open");
        };
        ws.onmessage = (e) => this.handle(e.data);
        ws.onclose = (e) => {
            this.ws = null;
            this.emit("close", e.code, e.reason);
            if (!this.closedByUser && this.opts.reconnect)
                this.scheduleReconnect();
        };
        ws.onerror = () => { }; // a close event always follows
    }
    scheduleReconnect() {
        const { baseDelayMs, maxDelayMs } = this.opts;
        const exp = Math.min(maxDelayMs, baseDelayMs * 2 ** this.attempt);
        const delay = Math.round(exp * (0.5 + Math.random() * 0.5)); // jitter
        this.attempt++;
        this.emit("reconnecting", this.attempt, delay);
        this.reconnectTimer = setTimeout(() => this.open(), delay);
    }
    handle(raw) {
        let m;
        try {
            m = JSON.parse(String(raw));
        }
        catch {
            return;
        }
        switch (m.type) {
            case "message":
                this.subs.get(m.topic)?.forEach((h) => h(m));
                this.emit("message", m);
                break;
            case "presence":
                this.emit("presence", m);
                break;
            case "who": {
                const waiters = this.whoWaiters;
                this.whoWaiters = [];
                waiters.forEach((w) => w(m.data));
                break;
            }
            case "error":
                this.emit("error", m);
                break;
            case "ok":
                break;
        }
    }
}
