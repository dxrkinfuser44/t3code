import * as NFS from "node:fs";
import { PassThrough } from "node:stream";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { FileSystem, Schema } from "effect";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { TestClock } from "effect/testing";
import { vi } from "vitest";

import { readBootstrapEnvelope, resolveFdPath } from "./bootstrap";
import { assertNone, assertSome } from "@effect/vitest/utils";

const fsInterceptor = vi.hoisted(() => ({
  failPath: null as string | null,
  silentReadFd: null as number | null,
}));

vi.mock("node:fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs")>();
  return {
    ...actual,
    openSync: (...args: Parameters<typeof actual.openSync>) => {
      const [filePath, flags] = args;
      if (typeof filePath === "string" && filePath === fsInterceptor.failPath && flags === "r") {
        const error = new Error("no such device or address");
        Object.assign(error, { code: "ENXIO" });
        throw error;
      }
      return (actual.openSync as (...a: typeof args) => number)(...args);
    },
    createReadStream: (...args: Parameters<typeof actual.createReadStream>) => {
      const [filePath, options] = args;
      const fd = (options as { fd?: number } | undefined)?.fd;
      if (
        typeof filePath === "string" &&
        filePath === "" &&
        typeof fd === "number" &&
        fd === fsInterceptor.silentReadFd
      ) {
        return new PassThrough({ encoding: "utf8" }) as unknown as ReturnType<
          typeof actual.createReadStream
        >;
      }

      return (
        actual.createReadStream as (...a: typeof args) => ReturnType<typeof actual.createReadStream>
      )(...args);
    },
  };
});

const TestEnvelopeSchema = Schema.Struct({ mode: Schema.String });

it.layer(NodeServices.layer)("readBootstrapEnvelope", (it) => {
  it.effect("uses platform-specific fd paths", () =>
    Effect.sync(() => {
      assert.equal(resolveFdPath(3, "linux"), "/proc/self/fd/3");
      assert.equal(resolveFdPath(3, "darwin"), "/dev/fd/3");
      assert.equal(resolveFdPath(3, "win32"), undefined);
    }),
  );

  it.effect("reads a bootstrap envelope from a provided fd", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const filePath = yield* fs.makeTempFileScoped({ prefix: "t3-bootstrap-", suffix: ".ndjson" });

      yield* fs.writeFileString(
        filePath,
        `${yield* Schema.encodeEffect(Schema.fromJsonString(TestEnvelopeSchema))({
          mode: "desktop",
        })}\n`,
      );

      // Stream ownership varies by platform/path strategy (duplicated fd vs direct fd).
      // Avoid explicit close to prevent racing the stream's auto-close semantics.
      const fd = NFS.openSync(filePath, "r");
      const payload = yield* readBootstrapEnvelope(TestEnvelopeSchema, fd, { timeoutMs: 100 });
      assertSome(payload, {
        mode: "desktop",
      });
    }),
  );

  it.effect("falls back to reading the inherited fd when path duplication fails", () =>
    Effect.gen(function* () {
      if (process.platform === "win32") {
        // Windows does not use /proc/self/fd or /dev/fd duplication paths.
        return;
      }

      const fs = yield* FileSystem.FileSystem;
      const filePath = yield* fs.makeTempFileScoped({ prefix: "t3-bootstrap-", suffix: ".ndjson" });

      yield* fs.writeFileString(
        filePath,
        `${yield* Schema.encodeEffect(Schema.fromJsonString(TestEnvelopeSchema))({
          mode: "desktop",
        })}\n`,
      );

      // Open without acquireRelease: the direct-stream fallback uses autoClose: true,
      // so the stream owns the fd lifecycle and closes it asynchronously on end.
      // Attempting to also close it synchronously in a finalizer races with the
      // stream's async close and produces an uncaught EBADF.
      const fd = NFS.openSync(filePath, "r");
      const fdPath = resolveFdPath(fd);
      if (fdPath === undefined) {
        throw new Error("Expected fd path duplication support on non-win32 platform.");
      }

      fsInterceptor.failPath = fdPath;
      try {
        const payload = yield* readBootstrapEnvelope(TestEnvelopeSchema, fd, { timeoutMs: 100 });
        assertSome(payload, {
          mode: "desktop",
        });
      } finally {
        fsInterceptor.failPath = null;
      }
    }),
  );

  it.effect("returns none when the fd is unavailable", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const filePath = yield* fs.makeTempFileScoped({ prefix: "t3-bootstrap-", suffix: ".ndjson" });

      const fd = NFS.openSync(filePath, "r");
      NFS.closeSync(fd);

      const payload = yield* readBootstrapEnvelope(TestEnvelopeSchema, fd, { timeoutMs: 100 });
      assertNone(payload);
    }),
  );

  it.effect("returns none when the bootstrap read times out before any value arrives", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const filePath = yield* fs.makeTempFileScoped({ prefix: "t3-bootstrap-", suffix: ".ndjson" });

      const fd = yield* Effect.acquireRelease(
        Effect.sync(() => NFS.openSync(filePath, "r")),
        (fd) => Effect.sync(() => NFS.closeSync(fd)),
      );
      fsInterceptor.silentReadFd = fd;

      try {
        const fiber = yield* readBootstrapEnvelope(TestEnvelopeSchema, fd, {
          timeoutMs: 100,
        }).pipe(Effect.forkScoped);

        yield* Effect.yieldNow;
        yield* TestClock.adjust(Duration.millis(100));

        const payload = yield* Fiber.join(fiber);
        assertNone(payload);
      } finally {
        fsInterceptor.silentReadFd = null;
      }
    }).pipe(Effect.provide(TestClock.layer())),
  );
});
