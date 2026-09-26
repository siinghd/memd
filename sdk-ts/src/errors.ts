/**
 * Typed errors. Every failure the client raises is a `MemdError`; HTTP
 * statuses map to a subclass so callers can branch with `instanceof`.
 *
 * The server's error body is `{"detail": ..., "code": ...}`: `detail` is the
 * human text (for 422, pydantic's list of issues) and `code` is the
 * machine-readable reason, which becomes `MemdError.code`. A body without a
 * `code` (an older server, a proxy) gets the server's default code for the
 * status.
 */

/**
 * Machine-readable error codes. The server's codes, plus `network_error`,
 * `timeout` and `aborted` for failures without a response and
 * `invalid_response` for a 2xx body that is not JSON. Open-ended: a newer
 * server may send codes this list does not name yet.
 */
export type MemdErrorCode =
  | "validation_error"
  | "unauthorized"
  | "forbidden"
  | "not_found"
  | "method_not_allowed"
  | "conflict"
  | "preview_mismatch"
  | "gone"
  | "namespace_destroyed"
  | "payload_too_large"
  | "rate_limited"
  | "internal_error"
  | "unavailable"
  | "error"
  | "invalid_response"
  | "network_error"
  | "timeout"
  | "aborted"
  | (string & {});

/** The server's default `code` per status (`_ERROR_CODES` in http.py). */
const DEFAULT_CODES: Record<number, MemdErrorCode> = {
  400: "validation_error",
  401: "unauthorized",
  403: "forbidden",
  404: "not_found",
  405: "method_not_allowed",
  409: "conflict",
  410: "gone",
  413: "payload_too_large",
  422: "validation_error",
  429: "rate_limited",
  500: "internal_error",
  503: "unavailable",
};

/** One pydantic validation issue from a 422 body. */
export interface ValidationIssue {
  type: string;
  loc: Array<string | number>;
  msg: string;
  input?: unknown;
  ctx?: Record<string, unknown>;
}

export interface MemdErrorInit {
  status: number;
  code: MemdErrorCode;
  message: string;
  /** The raw `detail` field of the error body (or the body text). */
  detail?: unknown;
  /** Seconds, from a `Retry-After` response header. */
  retryAfter?: number;
  cause?: unknown;
}

export class MemdError extends Error {
  /** HTTP status; `0` when no response was received (network, timeout, abort). */
  readonly status: number;
  /** The server's machine-readable `code` (see `MemdErrorCode`). */
  readonly code: MemdErrorCode;
  readonly detail: unknown;
  /** Seconds to wait before retrying, when the server sent `Retry-After`. */
  readonly retryAfter: number | undefined;

  constructor(init: MemdErrorInit) {
    super(init.message, init.cause !== undefined ? { cause: init.cause } : undefined);
    this.name = "MemdError";
    this.status = init.status;
    this.code = init.code;
    this.detail = init.detail;
    this.retryAfter = init.retryAfter;
  }
}

/** 400: an engine-boundary guard rejected the input (for example `meta` over 64 KiB). */
export class BadRequestError extends MemdError {
  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "BadRequestError";
  }
}

/** 401: the bearer key is missing or invalid. */
export class AuthenticationError extends MemdError {
  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "AuthenticationError";
  }
}

/** 403: the key is valid but not for this namespace, user or operation. */
export class PermissionDeniedError extends MemdError {
  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "PermissionDeniedError";
  }
}

/** 404: no such record or namespace (or a record the key's pinned user may not see). */
export class NotFoundError extends MemdError {
  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "NotFoundError";
  }
}

/** 409: conflicting state. */
export class ConflictError extends MemdError {
  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "ConflictError";
  }
}

/**
 * 409 `preview_mismatch`: a confirmed `forget` would delete a different set
 * than the preview whose fingerprint it carried. Nothing was deleted;
 * preview again and confirm that.
 */
export class ForgetPreviewMismatchError extends ConflictError {
  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "ForgetPreviewMismatchError";
  }
}

/** 410: the namespace was destroyed while the request was in flight. */
export class GoneError extends MemdError {
  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "GoneError";
  }
}

/** 413: the request body is over the server's 8 MiB cap. */
export class PayloadTooLargeError extends MemdError {
  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "PayloadTooLargeError";
  }
}

/** 422: the body failed schema validation. `issues` lists every problem. */
export class ValidationError extends MemdError {
  readonly issues: ValidationIssue[];

  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "ValidationError";
    this.issues = Array.isArray(init.detail) ? (init.detail as ValidationIssue[]) : [];
  }
}

/** 429: a per-key, per-namespace, maintenance or auth-failure budget is spent. */
export class RateLimitError extends MemdError {
  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "RateLimitError";
  }
}

/** 5xx: the server failed (500) or the namespace is briefly unavailable (503). */
export class ServerError extends MemdError {
  constructor(init: MemdErrorInit) {
    super(init);
    this.name = "ServerError";
  }
}

/** No response: DNS, connection refused or reset, or the body stream broke. */
export class NetworkError extends MemdError {
  constructor(message: string, cause?: unknown, code: "network_error" | "timeout" = "network_error") {
    super({ status: 0, code, message, cause });
    this.name = "NetworkError";
  }
}

/** The request exceeded `timeoutMs`. */
export class RequestTimeoutError extends NetworkError {
  constructor(timeoutMs: number, cause?: unknown) {
    super(`request timed out after ${timeoutMs} ms`, cause, "timeout");
    this.name = "RequestTimeoutError";
  }
}

/** The caller's `AbortSignal` fired. Never retried. */
export class RequestAbortedError extends MemdError {
  constructor(cause?: unknown) {
    super({ status: 0, code: "aborted", message: "request aborted", cause });
    this.name = "RequestAbortedError";
  }
}

/** Parse a `Retry-After` header (delta-seconds or an HTTP date) into seconds. */
export function parseRetryAfter(value: string | null | undefined, now: number = Date.now()): number | undefined {
  if (value == null || value.trim() === "") return undefined;
  const v = value.trim();
  if (/^\d+(\.\d+)?$/.test(v)) return Number(v);
  const at = Date.parse(v);
  if (Number.isNaN(at)) return undefined;
  return Math.max(0, (at - now) / 1000);
}

function messageFrom(status: number, detail: unknown): string {
  if (typeof detail === "string" && detail) return detail;
  if (Array.isArray(detail) && detail.length > 0) {
    return detail
      .map((d) => {
        const issue = d as Partial<ValidationIssue>;
        const loc = (issue.loc ?? []).filter((p) => p !== "body").join(".");
        return loc ? `${loc}: ${issue.msg ?? "invalid"}` : String(issue.msg ?? "invalid");
      })
      .join("; ");
  }
  return `HTTP ${status}`;
}

/** Build the typed error for a non-2xx response from its status, body text and headers. */
export function errorFromResponse(status: number, bodyText: string, headers: { get(name: string): string | null }): MemdError {
  let detail: unknown = bodyText;
  let code: unknown;
  try {
    const parsed: unknown = JSON.parse(bodyText);
    if (parsed && typeof parsed === "object" && "detail" in parsed) {
      detail = (parsed as { detail: unknown }).detail;
      code = (parsed as { code?: unknown }).code;
    } else {
      detail = parsed;
    }
  } catch {
    // not JSON: keep the text (proxies and gateways answer in HTML/plain text)
  }
  const init: MemdErrorInit = {
    status,
    code: typeof code === "string" && code ? code : (DEFAULT_CODES[status] ?? "error"),
    message: messageFrom(status, detail),
    detail,
    retryAfter: parseRetryAfter(headers.get("retry-after")),
  };
  switch (status) {
    case 400:
      return new BadRequestError(init);
    case 401:
      return new AuthenticationError(init);
    case 403:
      return new PermissionDeniedError(init);
    case 404:
      return new NotFoundError(init);
    case 409:
      return init.code === "preview_mismatch" ? new ForgetPreviewMismatchError(init) : new ConflictError(init);
    case 410:
      return new GoneError(init);
    case 413:
      return new PayloadTooLargeError(init);
    case 422:
      return new ValidationError(init);
    case 429:
      return new RateLimitError(init);
    default:
      if (status >= 500) return new ServerError(init);
      return new MemdError(init);
  }
}
