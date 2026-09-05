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
  return win.sessionStorage.getItem(TOKEN_STORAGE_KEY);
}

/** The token in force for this tab, or `null`. */
export function currentToken(storage: TokenWindow["sessionStorage"]): string | null {
  return storage.getItem(TOKEN_STORAGE_KEY);
}
