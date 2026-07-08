import init, { Core, init_logging } from "../../core/pkg/browser_app_core";

const $ = (id: string) => document.getElementById(id)!;
const log = (line: string) => {
  const el = $("log");
  el.textContent += `${new Date().toISOString().slice(11, 19)} ${line}\n`;
  el.scrollTop = el.scrollHeight;
};

let core: Core | undefined;

$("connect").addEventListener("click", async () => {
  await init();
  init_logging(); // tracing → console браузера
  core = new Core(($("url") as HTMLInputElement).value);
  core.on_state((s: string) => {
    $("status").textContent = s;
    log(`state: ${s}`);
  });
  ($("unary") as HTMLButtonElement).disabled = false;
  ($("stream") as HTMLButtonElement).disabled = false;
  ($("reconnect") as HTMLButtonElement).disabled = false;
  log("core created (lazy channel: connection opens on first call)");
});

$("reconnect").addEventListener("click", () => {
  core?.reconnect_now();
  log("reconnect_now()");
});

$("unary").addEventListener("click", async () => {
  if (!core) return;
  const text = ($("text") as HTMLInputElement).value;
  try {
    const echoed = await core.unary(text);
    log(`unary ok: ${echoed}`);
  } catch (e) {
    log(`unary error: ${e}`);
  }
});

$("stream").addEventListener("click", async () => {
  if (!core) return;
  try {
    await core.stream(10, (n: number) => log(`stream item: ${n}`));
    log("stream done");
  } catch (e) {
    log(`stream error: ${e}`);
  }
});
