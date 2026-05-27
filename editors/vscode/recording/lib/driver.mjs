// A tiny keyboard-only action vocabulary for scenarios. Keyboard-driven on
// purpose: Playwright's Electron video has no visible mouse cursor, so
// click-based flows look like things happen by magic. Everything here drives
// VS Code the way a keyboard user would — command palette, quick open, typing.

export function makeDriver(win) {
  const pause = (ms) => win.waitForTimeout(ms);

  // Open the command palette (pre-filled with ">"), type a filter, optionally
  // run the top match. Leave `run` false to just *show* the filtered list.
  async function palette(query, { run = false, settle = 700 } = {}) {
    await win.keyboard.press("Meta+Shift+P");
    await pause(settle);
    if (query) await win.keyboard.type(query, { delay: 55 });
    await pause(settle);
    if (run) {
      await win.keyboard.press("Enter");
      await pause(settle);
    }
  }

  // Run a VS Code command by its title (e.g. "Rocky: Compile Models").
  async function command(title, opts = {}) {
    await palette(title, { run: true, ...opts });
  }

  // Quick-open a file by (fuzzy) name.
  async function openFile(name, { settle = 700 } = {}) {
    await win.keyboard.press("Meta+P");
    await pause(settle);
    await win.keyboard.type(name, { delay: 45 });
    await pause(settle + 400);
    await win.keyboard.press("Enter");
    await pause(settle);
  }

  // Reach into an out-of-process webview iframe. VS Code renders webviews in
  // nested iframes that Playwright flattens into `win.frames()`; find ours by a
  // DOM marker unique to that panel (a CSS selector or `text=` locator). Polls
  // until present. NOTE: clicks have no visible cursor in the Electron video,
  // so what the GIF shows is the *resulting* state change (badges appearing,
  // tab content switching) — keep that in mind when scripting interactions.
  async function webview(marker, { timeout = 10000 } = {}) {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      for (const f of win.frames()) {
        if (await f.locator(marker).count().catch(() => 0)) return f;
      }
      await pause(250);
    }
    throw new Error(`webview with marker '${marker}' not found within ${timeout}ms`);
  }

  // Click a control (by accessible name) inside the webview identified by
  // `marker`, then settle so the state change is captured.
  async function clickInWebview(marker, name, { settle = 900 } = {}) {
    const frame = await webview(marker);
    await frame.getByRole("button", { name }).first().click();
    await pause(settle);
  }

  return {
    win,
    pause,
    key: (combo) => win.keyboard.press(combo),
    type: (text, delay = 60) => win.keyboard.type(text, { delay }),
    escape: () => win.keyboard.press("Escape"),
    palette,
    command,
    openFile,
    webview,
    clickInWebview,
  };
}
