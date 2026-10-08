import { describe, expect, it, vi } from "vitest";
import { ApiError, SESSION_EXPIRED_EVENT, SESSION_EXPIRED_HINT, apiGet, apiPost } from "./api";

function answer(status: number, body: string): Response {
  return new Response(body, { status, headers: { "content-type": "application/json" } });
}

function lastCall(doFetch: ReturnType<typeof vi.fn>): [string, RequestInit] {
  return doFetch.mock.calls.at(-1) as unknown as [string, RequestInit];
}

describe("apiGet", () => {
  it("sends the session cookie and the UI header, and parses the payload", async () => {
    const doFetch = vi.fn(async () => answer(200, '{"engine_version":"1.74.0"}'));
    const meta = await apiGet<{ engine_version: string }>("meta", {
      fetch: doFetch as unknown as typeof fetch,
      base: "http://127.0.0.1:1",
    });
    expect(meta.engine_version).toBe("1.74.0");
    const [url, init] = lastCall(doFetch);
    expect(url).toBe("http://127.0.0.1:1/api/v1/meta");
    expect(init.method).toBe("GET");
    expect(init.credentials).toBe("same-origin");
    const headers = init.headers as Record<string, string>;
    expect(headers["X-Rocky-UI"]).toBe("1");
    // The page never holds the token, so it never sends one.
    expect(headers.Authorization).toBeUndefined();
  });

  it("surfaces the server's envelope as ApiError", async () => {
    const doFetch = vi.fn(async () =>
      answer(409, '{"code":"mutation_in_progress","message":"busy","remediation_hint":"wait"}'),
    );
    const failure = await apiGet("meta", { fetch: doFetch as unknown as typeof fetch }).catch(
      (e: unknown) => e,
    );
    expect(failure).toBeInstanceOf(ApiError);
    const error = failure as ApiError;
    expect(error.status).toBe(409);
    expect(error.envelope.code).toBe("mutation_in_progress");
    expect(error.envelope.remediation_hint).toBe("wait");
  });

  it("turns a 401 into the session-expired hint and announces it", async () => {
    const doFetch = vi.fn(async () =>
      answer(401, '{"code":"unauthorized","message":"missing bearer","remediation_hint":"x"}'),
    );
    const events = new EventTarget();
    const heard = vi.fn();
    events.addEventListener(SESSION_EXPIRED_EVENT, heard);
    const failure = (await apiGet("meta", {
      fetch: doFetch as unknown as typeof fetch,
      events,
    }).catch((e: unknown) => e)) as ApiError;
    expect(failure.status).toBe(401);
    expect(failure.envelope.code).toBe("unauthorized");
    expect(failure.envelope.remediation_hint).toBe(SESSION_EXPIRED_HINT);
    expect(heard).toHaveBeenCalledTimes(1);
  });

  it("wraps a bodiless refusal in an envelope too", async () => {
    const doFetch = vi.fn(async () => new Response("", { status: 502 }));
    const failure = await apiGet("meta", { fetch: doFetch as unknown as typeof fetch }).catch(
      (e: unknown) => e,
    );
    expect((failure as ApiError).envelope.code).toBe("http_502");
  });
});

describe("apiPost", () => {
  it("posts JSON with the cookie and X-Rocky-UI, never a principal or a bearer", async () => {
    const doFetch = vi.fn(async () => answer(202, '{"job_id":"job_1"}'));
    const accepted = await apiPost<{ job_id: string }>(
      "jobs/run",
      { model: "orders" },
      {
        fetch: doFetch as unknown as typeof fetch,
        headers: {
          "X-Rocky-Principal": "someone-else",
          Authorization: "Bearer leaked",
          "X-Extra": "kept",
        },
      },
    );
    expect(accepted.job_id).toBe("job_1");
    const [url, init] = lastCall(doFetch);
    expect(url).toBe("/api/v1/jobs/run");
    expect(init.method).toBe("POST");
    expect(init.credentials).toBe("same-origin");
    expect(init.body).toBe('{"model":"orders"}');
    const headers = init.headers as Record<string, string>;
    expect(headers["X-Rocky-UI"]).toBe("1");
    expect(headers["Content-Type"]).toBe("application/json");
    expect(headers["X-Extra"]).toBe("kept");
    const names = Object.keys(headers).map((name) => name.toLowerCase());
    expect(names).not.toContain("x-rocky-principal");
    expect(names).not.toContain("authorization");
  });
});
