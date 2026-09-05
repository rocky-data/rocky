import { describe, expect, it, vi } from "vitest";
import { ApiError, apiGet } from "./api";
import { TOKEN_STORAGE_KEY } from "./token";

function storageWith(token: string | null) {
  return { getItem: (k: string) => (k === TOKEN_STORAGE_KEY ? token : null) };
}

function answer(status: number, body: string): Response {
  return new Response(body, { status, headers: { "content-type": "application/json" } });
}

describe("apiGet", () => {
  it("sends the bearer token and parses the payload", async () => {
    const doFetch = vi.fn(async () => answer(200, '{"engine_version":"1.74.0"}'));
    const meta = await apiGet<{ engine_version: string }>("meta", {
      fetch: doFetch as unknown as typeof fetch,
      storage: storageWith("s3cret"),
      base: "http://127.0.0.1:1",
    });
    expect(meta.engine_version).toBe("1.74.0");
    const [url, init] = doFetch.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("http://127.0.0.1:1/api/v1/meta");
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer s3cret");
  });

  it("sends no Authorization header without a token", async () => {
    const doFetch = vi.fn(async () => answer(200, "{}"));
    await apiGet("meta", { fetch: doFetch as unknown as typeof fetch, storage: storageWith(null) });
    const [, init] = doFetch.mock.calls[0] as unknown as [string, RequestInit];
    expect((init.headers as Record<string, string>).Authorization).toBeUndefined();
  });

  it("surfaces the server's envelope as ApiError", async () => {
    const doFetch = vi.fn(async () =>
      answer(401, '{"code":"unauthorized","message":"missing bearer","remediation_hint":"pass it"}'),
    );
    const failure = await apiGet("meta", {
      fetch: doFetch as unknown as typeof fetch,
      storage: storageWith("wrong"),
    }).catch((e: unknown) => e);
    expect(failure).toBeInstanceOf(ApiError);
    const error = failure as ApiError;
    expect(error.status).toBe(401);
    expect(error.envelope.code).toBe("unauthorized");
    expect(error.envelope.remediation_hint).toBe("pass it");
  });

  it("wraps a bodiless refusal in an envelope too", async () => {
    const doFetch = vi.fn(async () => new Response("", { status: 502 }));
    const failure = await apiGet("meta", {
      fetch: doFetch as unknown as typeof fetch,
      storage: storageWith("t"),
    }).catch((e: unknown) => e);
    expect((failure as ApiError).envelope.code).toBe("http_502");
  });
});
