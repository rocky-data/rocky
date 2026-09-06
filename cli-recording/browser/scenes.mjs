// The browser beats of the fulfillment screencast (U3-A3).
//
// Each scene drives a live `rocky serve --ui` and is filmed by record.mjs.
// Two rules they all follow:
//
//   * Wait for a landmark, never a timer. A `waitForSelector` that fails is a
//     loud failure; a `waitForTimeout` that is too short is a blank frame in
//     the finished cut, which nobody notices until the edit.
//   * Reach the screens by URL, not by clicking through the shell. The lanes
//     are deep-linkable on purpose (U2-P1), and a recording that navigates by
//     address proves that as it goes.
//
// `pause` is the one deliberate timer: it is reading time for the viewer, not
// a wait for the page.

const pause = (page, ms) => page.waitForTimeout(ms);

/** The base of the printed URL, without the `/ui/...` path or the fragment. */
function origin(page) {
  const u = new URL(page.url());
  return `${u.protocol}//${u.host}`;
}

/** Go to a `/ui/...` path on the server this page is already talking to. */
async function goto(page, uiPath) {
  await page.goto(`${origin(page)}${uiPath}`, { waitUntil: "domcontentloaded" });
}

export const SCENES = {
  /**
   * Scene 5–6: the plan waiting for a person.
   *
   * The queue in the engine's order, then one plan: what it would break, why
   * policy stopped it, whether the spec moved under it, and the command that
   * would approve it. No control on the page changes anything.
   */
  review: {
    description: "the review queue, then one plan's detail",
    async run(page) {
      // The address the server printed carries `#token=…`; the SPA moves it to
      // sessionStorage and scrubs the bar. Wait for that to have happened
      // before filming anything — a frame with the token in it is a frame that
      // teaches the pattern of a real one.
      await page.waitForFunction(() => !window.location.hash.includes("token="));

      await goto(page, "/ui/review");
      await page.waitForSelector('section[aria-label="The review queue"]');
      await pause(page, 2500);

      // The first plan in the queue — the engine's own ranking, not the page's.
      const first = page.locator('section[aria-label="The review queue"] a').first();
      await first.waitFor();
      await first.click();

      await page.waitForSelector('section[aria-label="Why it needs a human"]');
      await pause(page, 2000);
      await page.waitForSelector('section[aria-label="How to approve"]');
      await page.locator('section[aria-label="How to approve"]').scrollIntoViewIfNeeded();
      await pause(page, 3000);
    },
  },

  /**
   * Scene 9: the rows, and the fact that nothing read them until asked.
   *
   * The panel is a button first. That is the whole point: a page that sampled
   * on load would spend warehouse money every time someone opened a link.
   */
  samples: {
    description: "the sample panel — a button first, then real rows",
    async run(page) {
      await page.waitForFunction(() => !window.location.hash.includes("token="));

      await goto(page, "/ui/review");
      const first = page.locator('section[aria-label="The review queue"] a').first();
      await first.waitFor();
      await first.click();

      const panel = page.locator('section[aria-label="Sample rows"]');
      await panel.waitFor();
      await panel.scrollIntoViewIfNeeded();
      // Hold on the un-asked state: the sentence that says what the click costs.
      await pause(page, 3000);

      await panel.getByRole("button").click();
      await panel.locator("table").waitFor();
      await pause(page, 3500);
    },
  },

  /**
   * Scene 11: the custody chain, which is the journal.
   *
   * A real one is long — 82 rows for one product through one loop — so this
   * scrolls it rather than pretending it fits.
   */
  journal: {
    description: "one product's whole life, in append order",
    async run(page) {
      await page.waitForFunction(() => !window.location.hash.includes("token="));

      await goto(page, "/ui/governor/products");
      await page.waitForSelector("text=/\\d+ products?/");
      await pause(page, 2000);

      const first = page.locator("main a, body a").filter({ hasText: /^[a-z0-9_]+$/ }).first();
      await first.waitFor();
      await first.click();

      await page.waitForSelector('section[aria-label="Where it stands"]');
      await pause(page, 2500);

      const journal = page.locator('section[aria-label="What happened"]');
      await journal.waitFor();
      await journal.scrollIntoViewIfNeeded();
      await pause(page, 2000);

      // Read it the way a person would: down the table, not in one jump.
      for (let i = 0; i < 6; i += 1) {
        await page.mouse.wheel(0, 400);
        await pause(page, 700);
      }
      await pause(page, 2000);
    },
  },
};
