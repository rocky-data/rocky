import { describe, expect, it } from "vitest";
import {
  TOKEN_STORAGE_KEY,
  bootstrapToken,
  currentToken,
  tokenFromFragment,
  type TokenWindow,
} from "./token";

function fakeWindow(hash: string, stored: string | null = null): TokenWindow & {
  replaced: string[];
  store: Map<string, string>;
} {
  const store = new Map<string, string>();
  if (stored !== null) store.set(TOKEN_STORAGE_KEY, stored);
  const replaced: string[] = [];
  return {
    location: { hash, pathname: "/ui/", search: "" },
    history: {
      replaceState: (_data, _unused, url) => {
        replaced.push(url ?? "");
      },
    },
    sessionStorage: {
      getItem: (k) => store.get(k) ?? null,
      setItem: (k, v) => {
        store.set(k, v);
      },
    },
    replaced,
    store,
  };
}

describe("tokenFromFragment", () => {
  it("reads #token=<secret> and nothing else", () => {
    expect(tokenFromFragment("#token=s3cret")).toBe("s3cret");
    expect(tokenFromFragment("token=s3cret")).toBe("s3cret");
    expect(tokenFromFragment("#other=1")).toBeNull();
    expect(tokenFromFragment("#token=")).toBeNull();
    expect(tokenFromFragment("")).toBeNull();
  });

  it("decodes a percent-encoded secret", () => {
    expect(tokenFromFragment("#token=a%2Fb%3Dc")).toBe("a/b=c");
  });
});

describe("bootstrapToken", () => {
  it("stores the fragment token and scrubs the address", () => {
    const win = fakeWindow("#token=s3cret");
    expect(bootstrapToken(win)).toBe("s3cret");
    expect(win.store.get(TOKEN_STORAGE_KEY)).toBe("s3cret");
    expect(win.replaced).toEqual(["/ui/"]);
  });

  it("falls back to the stored token when the fragment has none", () => {
    const win = fakeWindow("", "kept");
    expect(bootstrapToken(win)).toBe("kept");
    expect(win.replaced).toEqual([]);
  });

  it("is null with neither, and touches nothing", () => {
    const win = fakeWindow("#estate");
    expect(bootstrapToken(win)).toBeNull();
    expect(win.store.size).toBe(0);
    expect(win.replaced).toEqual([]);
  });
});

describe("an empty stored token", () => {
  // Three readers ask "is there a token", and they must not disagree. `apiGet`
  // sends no Authorization header for "" because it is falsy, so any reader
  // that called "" a token would mount the lanes and let them fetch without
  // credentials -- the wall of 401s the shell's gate exists to remove.
  it("is not a token, to currentToken", () => {
    expect(currentToken(fakeWindow("", "").sessionStorage)).toBeNull();
    expect(currentToken(fakeWindow("", "real").sessionStorage)).toBe("real");
  });

  it("is not a token, to bootstrapToken either", () => {
    const win = fakeWindow("", "");
    expect(bootstrapToken(win)).toBeNull();
    // And the address is left alone: there was no fragment to scrub.
    expect(win.replaced).toEqual([]);
  });

  it("never reaches storage from a fragment in the first place", () => {
    const win = fakeWindow("#token=");
    expect(bootstrapToken(win)).toBeNull();
    expect(win.store.has(TOKEN_STORAGE_KEY)).toBe(false);
  });
});
