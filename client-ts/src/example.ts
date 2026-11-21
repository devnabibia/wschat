// Interactive CLI:  npm run example -- alice general
// Type a line to publish. Commands: /who, /quit
import * as readline from "node:readline";
import { ChatClient } from "./client.js";

const [user = "alice", room = "general"] = process.argv.slice(2);
const url = process.env.WS_URL ?? "ws://localhost:8080/ws";

const client = new ChatClient({ url, user });

client.on("reconnecting", (n, ms) => console.log(`* reconnecting (#${n}) in ${ms}ms`));
client.on("close", (code) => console.log(`* disconnected (${code})`));
client.on("error", (e) => console.log(`! ${e.data}`));

client.subscribe<string>(room, (m) => {
  const time = new Date(m.ts).toLocaleTimeString();
  console.log(`[${time}] ${m.from}: ${m.data}`);
});
client.onPresence((p) => console.log(`* ${p.from} is ${p.data}`));

await client.connect();
console.log(`connected as ${user}, room "${room}". online: ${(await client.who()).join(", ")}`);

const rl = readline.createInterface({ input: process.stdin });
rl.on("line", async (line) => {
  const text = line.trim();
  if (!text) return;
  if (text === "/quit") {
    client.close();
    rl.close();
  } else if (text === "/who") {
    console.log("online:", (await client.who()).join(", "));
  } else if (!client.publish(room, text)) {
    console.log("! not connected, message not sent");
  }
});
