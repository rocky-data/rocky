# Browser recording

Records the Rocky browser UI (`rocky serve --ui`) with Playwright, for the
fulfillment screencast. The terminal half of that screencast is `vhs` and the
`.tape` files one directory up; this is the other half.

Nothing here ships. It is a private `package.json` outside `engine/ui`'s
dependency tree, the same arrangement
[`editors/vscode/recording/`](../../editors/vscode/recording/) uses for the
extension demos.

## Usage

Normally you do not run this directly — `../record-screencast.sh` drives the
tapes and these scenes in order against one workspace, which is what makes the
plan id on screen the same in both halves. To run a scene on its own:

```bash
cd cli-recording/browser
npm install
npx playwright install chromium     # once; ~95 MB

# In another shell, with a project that has a plan waiting for review:
ROCKY_SERVE_TOKEN=… ROCKY_SERVE_TOKEN_SCOPE=read-only rocky serve --ui --port 18733
# it prints:  http://127.0.0.1:18733/ui/#token=…

node record.mjs review  --url 'http://127.0.0.1:18733/ui/#token=…'
node record.mjs samples --url '…'
node record.mjs journal --url '…'
```

Each scene writes `out/<scene>.webm`. `out/` is generated; it is gitignored.

## What the scenes are

| Scene | Beat | Screencast scene |
|---|---|---|
| `review` | The queue in the engine's order, then one plan: what it would break, why policy stopped it, whether the spec moved, and the command that would approve it | 5–6 |
| `samples` | The sample panel as a button first, then real rows | 9 |
| `journal` | One product's whole life in append order | 11 |

## Three things it does on purpose

**It waits for a landmark, never for a timer.** A `waitForSelector` that fails
is a loud failure. A `waitForTimeout` that is too short is a blank frame in the
finished cut, which nobody notices until the edit. The only timers are reading
time for the viewer.

**It navigates by address.** The lanes are deep-linkable, so the recording
proves that as it goes rather than clicking through the shell.

**It waits for the token to leave the address bar before filming.**
`rocky serve --ui` prints `http://127.0.0.1:<port>/ui/#token=<secret>`. The SPA
reads that fragment once, moves it to `sessionStorage`, and rewrites the
address without it (`engine/ui/src/token.ts`). Every scene waits for the scrub
first, so no frame carries the secret. Use an obviously fake token when
recording anyway.

**A page error fails the run.** A console error or an uncaught exception exits
non-zero even when a `.webm` exists, because frames recorded through a broken
page are not evidence of anything.

## The gotcha, inherited

Take the video handle **before** `close()`:

```js
const video = page.video();   // handle first
await context.close();        // then close
await video.path();           // path() only resolves after the close
```

Reversed, `path()` never resolves. That one cost an iteration on the VS Code
harness; it is the same Playwright behaviour here.
