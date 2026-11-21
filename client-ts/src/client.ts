import {
  PRESENCE_TOPIC,
  type ChatMessage,
  type ClientMessage,
  type PresenceEvent,
  type ServerError,
  type ServerMessage,
} from "./protocol.js";

export interface ClientOptions {
  /** e.g. "ws://localhost:8080/ws" (no query string) */
  url: string;
  user: string;
  /** Auto-reconnect with exponential backoff + jitter. Default: true */
  reconnect?: boolean;
  baseDelayMs?: number; // default 500
  maxDelayMs?: number; // default 15000
  /** Inject a WebSocket implementation (older Node, tests). */
  WebSocketImpl?: typeof WebSocket;
}

export type MessageHandler<T = unknown> = (msg: ChatMessage<T>) => void;

interface Events {
  open: [];
  close: [code: number, reason: string];
  reconnecting: [attempt: number, delayMs: number];
  message: [msg: ChatMessage];
  presence: [evt: PresenceEvent];
  error: [err: ServerError];
}

const OPEN = 1; // WebSocket.OPEN

export class ChatClient {
  private readonly opts: Required<Omit<ClientOptions, "WebSocketImpl">> &
    Pick<ClientOptions, "WebSocketImpl">;
  private ws: WebSocket | null = null;
  private attempt = 0;
  private closedByUser = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;

  // topic -> handlers. Its keys are replayed to the server after every reconnect.
  private subs = new Map<string, Set<MessageHandler>>();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private listeners = new Map<keyof Events, Set<(...args: any[]) => void>>();
  private whoWaiters: Array<(users: string[]) => void> = [];

  constructor(opts: ClientOptions) {
    this.opts = {
      reconnect: true,
      baseDelayMs: 500,
      maxDelayMs: 15_000,
      ...opts,
    };
  }

  get connected(): boolean {
    return this.ws?.readyState === OPEN;
  }

  /**
   * Resolves on the first successful open. With reconnect enabled, it keeps
   * retrying in the background until the server is reachable.
   */
  connect(): Promise<void> {
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

  close(): void {
    this.closedByUser = true;
    clearTimeout(this.reconnectTimer);
    this.ws?.close();
  }

  /** Subscribe to a topic. Returns an unsubscribe function. */
  subscribe<T = unknown>(topic: string, handler: MessageHandler<T>): () => void {
    let set = this.subs.get(topic);
    if (!set) {
      set = new Set();
      this.subs.set(topic, set);
      this.send({ type: "subscribe", topic }); // no-op if offline; replayed on open
    }
    set.add(handler as MessageHandler);

    return () => {
      const s = this.subs.get(topic);
      if (!s) return;
      s.delete(handler as MessageHandler);
      if (s.size === 0) {
        this.subs.delete(topic);
        this.send({ type: "unsubscribe", topic });
      }
    };
  }

  /** Publish JSON-serializable data. Returns false if not currently connected. */
  publish<T>(topic: string, data: T): boolean {
    return this.send({ type: "publish", topic, data });
  }

  /** Be notified when users go online/offline. Returns a stop function. */
  onPresence(fn: (evt: PresenceEvent) => void): () => void {
    const off = this.on("presence", fn);
    const unsub = this.subscribe(PRESENCE_TOPIC, () => {});
    return () => {
      off();
      unsub();
    };
  }

  /** Ask the server who is online right now. */
  who(timeoutMs = 5000): Promise<string[]> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.whoWaiters = this.whoWaiters.filter((w) => w !== waiter);
        reject(new Error("who() timed out"));
      }, timeoutMs);
      const waiter = (users: string[]) => {
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

  on<K extends keyof Events>(event: K, fn: (...args: Events[K]) => void): () => void {
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

  private emit<K extends keyof Events>(event: K, ...args: Events[K]): void {
    this.listeners.get(event)?.forEach((fn) => fn(...args));
  }

  private send(msg: ClientMessage): boolean {
    if (!this.ws || this.ws.readyState !== OPEN) return false;
    this.ws.send(JSON.stringify(msg));
    return true;
  }

  private open(): void {
    const Impl = this.opts.WebSocketImpl ?? WebSocket;
    const ws = new Impl(`${this.opts.url}?user=${encodeURIComponent(this.opts.user)}`);
    this.ws = ws;

    ws.onopen = () => {
      this.attempt = 0;
      for (const topic of this.subs.keys()) this.send({ type: "subscribe", topic });
      this.emit("open");
    };
    ws.onmessage = (e: MessageEvent) => this.handle(e.data);
    ws.onclose = (e: CloseEvent) => {
      this.ws = null;
      this.emit("close", e.code, e.reason);
      if (!this.closedByUser && this.opts.reconnect) this.scheduleReconnect();
    };
    ws.onerror = () => {}; // a close event always follows
  }

  private scheduleReconnect(): void {
    const { baseDelayMs, maxDelayMs } = this.opts;
    const exp = Math.min(maxDelayMs, baseDelayMs * 2 ** this.attempt);
    const delay = Math.round(exp * (0.5 + Math.random() * 0.5)); // jitter
    this.attempt++;
    this.emit("reconnecting", this.attempt, delay);
    this.reconnectTimer = setTimeout(() => this.open(), delay);
  }

  private handle(raw: unknown): void {
    let m: ServerMessage;
    try {
      m = JSON.parse(String(raw)) as ServerMessage;
    } catch {
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
