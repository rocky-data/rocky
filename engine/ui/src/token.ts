/**
 * The UI token: how the browser gets the bearer secret `rocky serve --ui`
 * expects on every API call.
 *
 * `rocky serve --ui` prints one address, `http://127.0.0.1:<port>/ui/#token=<secret>`.
 * The fragment never reaches the server (browsers do not send it), so the
 * secret is not in any access log. On load the SPA reads it once, keeps it
 * in `sessionStorage` (per tab, gone when the tab closes), and rewrites the
 * address without it so a copied link or a screenshot does not carry it.
 */

export const TOKEN_STORAGE_KEY = "rocky.ui.token";

/** The narrow slice of `window` the bootstrap touches, so tests can fake it. */
export interface TokenWindow {
  location: { hash: string; pathname: string; search: string };
  history: { replaceState: (data: unknown, unused: string, url?: string) => void };
  sessionStorage: { getItem: (k: string) => string | null; setItem: (k: string, v: string) => void };
}

/** Parse `#token=<secret>` (and nothing else) out of a fragment. */
export function tokenFromFragment(hash: string): string | null {
  const raw = hash.startsWith("#") ? hash.slice(1) : hash;
  if (raw === "") return null;
  const params = new URLSearchParams(raw);
  const token = params.get("token");
  return token && token.length > 0 ? token : null;
}

/**
 * Move a fragment token into session storage and scrub the address. Returns
 * the token now in force, or `null` when neither the fragment nor storage
 * has one (the engine panel then shows how to start the server).
 */
export function bootstrapToken(win: TokenWindow): string | null {
  const fromFragment = tokenFromFragment(win.location.hash);
  if (fromFragment !== null) {
    win.sessionStorage.setItem(TOKEN_STORAGE_KEY, fromFragment);
    win.history.replaceState(null, "", win.location.pathname + win.location.search);
    return fromFragment;
  }
  // Through `currentToken`, not straight out of storage: an empty stored
  // value is not a token, and every reader has to agree about that.
  return currentToken(win.sessionStorage);
}

/**
 * The token in force for this tab, or `null`.
 *
 * An empty stored value is `null`, not a token. Two callers ask this question
 * and they must not disagree: the shell decides whether to render a lane at
 * all, and `apiGet` decides whether to send an `Authorization` header. `""` is
 * falsy, so `apiGet` would send no header — a shell that read `""` as "have a
 * token" would mount every lane and fetch without credentials, which is the
 * wall of `401`s this gate exists to prevent. `tokenFromFragment` already
 * refuses a zero-length token; this is the same rule on the way out.
 */
export function currentToken(storage: TokenWindow["sessionStorage"]): string | null {
  const token = storage.getItem(TOKEN_STORAGE_KEY);
  return token !== null && token.length > 0 ? token : null;
}
