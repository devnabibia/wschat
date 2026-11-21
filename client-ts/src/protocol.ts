// Wire protocol, mirroring protocol.go on the server.

/** Client -> server */
export type ClientMessage =
  | { type: "subscribe"; topic: string }
  | { type: "unsubscribe"; topic: string }
  | { type: "publish"; topic: string; data: unknown }
  | { type: "who" };

/** A message published to a topic by some user. */
export interface ChatMessage<T = unknown> {
  type: "message";
  topic: string;
  from: string;
  data: T;
  ts: number; // unix millis, set by the server
}

export interface PresenceEvent {
  type: "presence";
  topic: "presence";
  from: string; // the user whose status changed
  data: "online" | "offline";
  ts: number;
}

export interface WhoReply {
  type: "who";
  data: string[];
}

export interface OkReply {
  type: "ok";
  topic?: string;
  data: string;
}

export interface ServerError {
  type: "error";
  topic?: string;
  data: string;
}

/** Server -> client (discriminated on `type`) */
export type ServerMessage =
  | ChatMessage
  | PresenceEvent
  | WhoReply
  | OkReply
  | ServerError;

export const PRESENCE_TOPIC = "presence";
