import { describe, it, assert } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/unstable/process";

import { checkCopilotProviderStatus } from "./CopilotProvider";
import { ServerSettingsService } from "../../serverSettings";

const missingBinarySpawnerLayer = Layer.succeed(
  ChildProcessSpawner.ChildProcessSpawner,
  ChildProcessSpawner.make(() =>
    Effect.fail(
      PlatformError.systemError({
        _tag: "NotFound",
        module: "ChildProcess",
        method: "spawn",
        description: "copilot-missing-binary-test",
      }),
    ),
  ),
);

describe("checkCopilotProviderStatus", () => {
  it.effect("reports unavailable/error when Copilot CLI binary is missing", () =>
    Effect.gen(function* () {
      const status = yield* checkCopilotProviderStatus();

      assert.equal(status.provider, "copilot");
      assert.equal(status.enabled, true);
      assert.equal(status.status, "error");
      assert.equal(status.installed, false);
      assert.deepEqual(status.auth, { status: "unknown" });
      assert.equal(
        status.message,
        "GitHub Copilot CLI (`copilot`) is not installed or not on PATH.",
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          ServerSettingsService.layerTest({
            providers: {
              copilot: {
                enabled: true,
                binaryPath: "copilot-missing-binary-test",
                customModels: [],
              },
            },
          }),
          missingBinarySpawnerLayer,
        ),
      ),
    ),
  );
});
