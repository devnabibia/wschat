// Browser demo built on the ChatClient SDK. Compiled to ../web/js/ and
// embedded into the Go binary, so `go run .` needs no Node toolchain.
import { ChatClient } from "./client.js";
const DEFAULT_USER = "nabibia";
const $ = (id) => document.getElementById(id);
const wsURL = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`;
let client = null;
let me = "";
let room = "";
const online = new Set();
// NOTE: remote data is only ever put in via textContent, never as HTML.
function pushToLog(el) {
    const log = $("log");
    log.appendChild(el);
    log.scrollTop = log.scrollHeight;
}
function addLine(text, cls) {
    const div = document.createElement("div");
    div.className = `note ${cls}`;
    div.textContent = text;
    pushToLog(div);
}
function addMessage(from, ts, text) {
    const mine = from === me;
    const wrap = document.createElement("div");
    wrap.className = mine ? "msg me" : "msg";
    const meta = document.createElement("div");
    meta.className = "meta";
    const time = new Date(ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    meta.textContent = `${mine ? "You" : from} · ${time}`;
    const body = document.createElement("div");
    body.className = "body";
    body.textContent = text;
    wrap.append(meta, body);
    pushToLog(wrap);
}
function hue(name) {
    let h = 0;
    for (const ch of name)
        h = (h * 31 + ch.charCodeAt(0)) % 360;
    return h;
}
function renderOnline() {
    const items = [...online].sort().map((u) => {
        const li = document.createElement("li");
        const av = document.createElement("span");
        av.className = "avatar";
        av.textContent = (u[0] ?? "?").toUpperCase();
        av.style.setProperty("--h", String(hue(u)));
        const name = document.createElement("span");
        name.className = "name";
        name.textContent = u;
        li.append(av, name);
        if (u === me) {
            const you = document.createElement("span");
            you.className = "you";
            you.textContent = "you";
            li.append(you);
        }
        return li;
    });
    $("online").replaceChildren(...items);
    $("count").textContent = String(online.size);
}
function setStatus(text, state) {
    $("status").textContent = text;
    $("status").dataset.state = state;
}
async function connect() {
    client?.close();
    online.clear();
    renderOnline();
    $("log").replaceChildren();
    me = $("user").value.trim() || DEFAULT_USER;
    room = $("room").value.trim() || "general";
    $("roomname").textContent = `# ${room}`;
    const c = new ChatClient({ url: wsURL, user: me });
    client = c;
    setStatus("connecting…", "warn");
    c.on("reconnecting", (n, ms) => setStatus(`reconnecting (#${n}) in ${ms}ms`, "warn"));
    c.on("close", () => setStatus("disconnected", "down"));
    c.on("error", (e) => addLine(`! ${e.data}`, "err"));
    c.on("open", async () => {
        setStatus(`connected as ${me}`, "up");
        try {
            online.clear(); // resync after (re)connect: we may have missed events
            (await c.who()).forEach((u) => online.add(u));
            renderOnline();
        }
        catch {
            /* ignore */
        }
    });
    c.subscribe(room, (m) => addMessage(m.from, m.ts, String(m.data)));
    c.onPresence((p) => {
        if (p.data === "online")
            online.add(p.from);
        else
            online.delete(p.from);
        renderOnline();
        addLine(`${p.from} went ${p.data}`, "sys");
    });
    await c.connect();
    $("msg").focus();
}
function send() {
    const input = $("msg");
    const text = input.value.trim();
    if (!text || !client)
        return;
    if (client.publish(room, text))
        input.value = "";
    else
        addLine("not connected, message not sent", "err");
}
$("user").value = DEFAULT_USER;
$("connect").addEventListener("click", () => void connect());
$("send").addEventListener("click", send);
$("msg").addEventListener("keydown", (e) => {
    if (e.key === "Enter")
        send();
});
void connect();
