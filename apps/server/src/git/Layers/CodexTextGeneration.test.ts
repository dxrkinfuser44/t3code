import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { Effect, FileSystem, Layer, Path, Result } from "effect";
import { expect } from "vitest";

import { ServerConfig } from "../../config.ts";
import { CodexTextGenerationLive } from "./CodexTextGeneration.ts";
import { TextGenerationError } from "@t3tools/contracts";
import { TextGeneration } from "../Services/TextGeneration.ts";
import { ServerSettingsService } from "../../serverSettings.ts";

const DEFAULT_TEST_MODEL_SELECTION = {
  provider: "codex" as const,
  model: "gpt-5.4-mini",
};

const CodexTextGenerationTestLayer = CodexTextGenerationLive.pipe(
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(
    ServerConfig.layerTest(process.cwd(), {
      prefix: "t3code-codex-text-generation-test-",
    }),
  ),
  Layer.provideMerge(NodeServices.layer),
);

function makeFakeCodexBinary(
  dir: string,
  input: {
    output: string;
    exitCode?: number;
    stderr?: string;
    requireImage?: boolean;
    requireFastServiceTier?: boolean;
    requireReasoningEffort?: string;
    forbidReasoningEffort?: boolean;
    stdinMustContain?: string;
    stdinMustNotContain?: string;
  },
) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const binDir = path.join(dir, "bin");
    const shimPath = path.join(binDir, "codex-shim.cjs");
    const expectations = {
      output: input.output,
      exitCode: input.exitCode,
      stderr: input.stderr,
      requireImage: input.requireImage,
      requireFastServiceTier: input.requireFastServiceTier,
      requireReasoningEffort: input.requireReasoningEffort,
      forbidReasoningEffort: input.forbidReasoningEffort,
      stdinMustContain: input.stdinMustContain,
      stdinMustNotContain: input.stdinMustNotContain,
    };
    yield* fs.makeDirectory(binDir, { recursive: true });

    yield* fs.writeFileString(
      shimPath,
      [
        'const fs = require("node:fs");',
        `const expectations = ${JSON.stringify(expectations)};`,
        "const args = process.argv.slice(2);",
        'const normalizeConfigValue = (value) => String(value).replaceAll("\\\"", "");',
        'let outputPath = "";',
        "let seenImage = false;",
        "let seenFastServiceTier = false;",
        'let seenReasoningEffort = "";',
        "for (let index = 0; index < args.length; index += 1) {",
        "  const arg = args[index];",
        '  if (arg === "--image") {',
        "    const imagePath = args[index + 1];",
        '    if (typeof imagePath === "string" && imagePath.length > 0) {',
        "      seenImage = true;",
        "    }",
        "    index += 1;",
        "    continue;",
        "  }",
        '  if (arg === "--config") {',
        '    const configValue = args[index + 1] ?? "";',
        "    const normalizedConfigValue = normalizeConfigValue(configValue);",
        '    if (normalizedConfigValue === "service_tier=fast") {',
        "      seenFastServiceTier = true;",
        "    }",
        '    if (normalizedConfigValue.startsWith("model_reasoning_effort=")) {',
        "      seenReasoningEffort = normalizedConfigValue;",
        "    }",
        "    index += 1;",
        "    continue;",
        "  }",
        '  if (arg === "--output-last-message") {',
        '    outputPath = args[index + 1] ?? "";',
        "    index += 1;",
        "  }",
        "}",
        'const stdinContent = fs.readFileSync(0, "utf8");',
        "if (expectations.requireImage && !seenImage) {",
        '  process.stderr.write("missing --image input\\n");',
        "  process.exit(2);",
        "}",
        "if (expectations.stdinMustContain !== undefined && !stdinContent.includes(expectations.stdinMustContain)) {",
        '  process.stderr.write("stdin missing expected content\\n");',
        "  process.exit(3);",
        "}",
        "if (expectations.stdinMustNotContain !== undefined && stdinContent.includes(expectations.stdinMustNotContain)) {",
        '  process.stderr.write("stdin contained forbidden content\\n");',
        "  process.exit(4);",
        "}",
        "if (expectations.requireFastServiceTier && !seenFastServiceTier) {",
        '  process.stderr.write("missing fast service tier config\\n");',
        "  process.exit(5);",
        "}",
        "if (expectations.requireReasoningEffort !== undefined) {",
        '  const expectedReasoningEffort = "model_reasoning_effort=" + expectations.requireReasoningEffort;',
        "  if (seenReasoningEffort !== expectedReasoningEffort) {",
        "    process.stderr.write(`unexpected reasoning effort config: ${seenReasoningEffort}\\n`);",
        "    process.exit(6);",
        "  }",
        "}",
        "if (expectations.forbidReasoningEffort && seenReasoningEffort.length > 0) {",
        "  process.stderr.write(`reasoning effort config should be omitted: ${seenReasoningEffort}\\n`);",
        "  process.exit(7);",
        "}",
        "if (expectations.stderr !== undefined) {",
        "  process.stderr.write(`${expectations.stderr}\\n`);",
        "}",
        "if (outputPath.length > 0) {",
        '  fs.writeFileSync(outputPath, expectations.output, "utf8");',
        "}",
        "process.exit(expectations.exitCode ?? 0);",
        "",
      ].join("\n"),
    );

    if (process.platform === "win32") {
      const codexCmdPath = path.join(binDir, "codex.cmd");
      yield* fs.writeFileString(
        codexCmdPath,
        ["@echo off", 'node "%~dp0codex-shim.cjs" %*', ""].join("\r\n"),
      );
      return codexCmdPath;
    }

    const codexPath = path.join(binDir, "codex");
    yield* fs.writeFileString(
      codexPath,
      ["#!/bin/sh", 'node "$(dirname "$0")/codex-shim.cjs" "$@"', ""].join("\n"),
    );
    yield* fs.chmod(codexPath, 0o755);
    return codexPath;
  });
}

function withFakeCodexEnv<A, E, R>(
  input: {
    output: string;
    exitCode?: number;
    stderr?: string;
    requireImage?: boolean;
    requireFastServiceTier?: boolean;
    requireReasoningEffort?: string;
    forbidReasoningEffort?: boolean;
    stdinMustContain?: string;
    stdinMustNotContain?: string;
  },
  effect: Effect.Effect<A, E, R>,
) {
  return Effect.acquireUseRelease(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-codex-text-" });
      const codexPath = yield* makeFakeCodexBinary(tempDir, input);
      const serverSettings = yield* ServerSettingsService;
      const previousSettings = yield* serverSettings.getSettings;
      yield* serverSettings.updateSettings({
        providers: {
          codex: {
            binaryPath: codexPath,
          },
        },
      });
      return { serverSettings, previousBinaryPath: previousSettings.providers.codex.binaryPath };
    }),
    () => effect,
    ({ serverSettings, previousBinaryPath }) =>
      serverSettings
        .updateSettings({
          providers: {
            codex: {
              binaryPath: previousBinaryPath,
            },
          },
        })
        .pipe(Effect.asVoid),
  );
}

it.layer(CodexTextGenerationTestLayer)("CodexTextGenerationLive", (it) => {
  it.effect("generates and sanitizes commit messages without branch by default", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          subject:
            "  Add important change to the system with too much detail and a trailing period.\nsecondary line",
          body: "\n- added migration\n- updated tests\n",
        }),
        stdinMustNotContain: "branch must be a short semantic git branch fragment",
      },
      Effect.gen(function* () {
        const textGeneration = yield* TextGeneration;

        const generated = yield* textGeneration.generateCommitMessage({
          cwd: process.cwd(),
          branch: "feature/codex-effect",
          stagedSummary: "M README.md",
          stagedPatch: "diff --git a/README.md b/README.md",
          modelSelection: DEFAULT_TEST_MODEL_SELECTION,
        });

        expect(generated.subject.length).toBeLessThanOrEqual(72);
        expect(generated.subject.endsWith(".")).toBe(false);
        expect(generated.body).toBe("- added migration\n- updated tests");
        expect(generated.branch).toBeUndefined();
      }),
    ),
  );

  it.effect(
    "forwards codex fast mode and non-default reasoning effort into codex exec config",
    () =>
      withFakeCodexEnv(
        {
          output: JSON.stringify({
            subject: "Add important change",
            body: "",
          }),
          requireFastServiceTier: true,
          requireReasoningEffort: "xhigh",
          stdinMustNotContain: "branch must be a short semantic git branch fragment",
        },
        Effect.gen(function* () {
          const textGeneration = yield* TextGeneration;

          yield* textGeneration.generateCommitMessage({
            cwd: process.cwd(),
            branch: "feature/codex-effect",
            stagedSummary: "M README.md",
            stagedPatch: "diff --git a/README.md b/README.md",
            modelSelection: {
              provider: "codex",
              model: "gpt-5.4",
              options: {
                reasoningEffort: "xhigh",
                fastMode: true,
              },
            },
          });
        }),
      ),
  );

  it.effect("defaults git text generation codex effort to low", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          subject: "Add important change",
          body: "",
        }),
        requireReasoningEffort: "low",
      },
      Effect.gen(function* () {
        const textGeneration = yield* TextGeneration;

        yield* textGeneration.generateCommitMessage({
          cwd: process.cwd(),
          branch: "feature/codex-effect",
          stagedSummary: "M README.md",
          stagedPatch: "diff --git a/README.md b/README.md",
          modelSelection: DEFAULT_TEST_MODEL_SELECTION,
        });
      }),
    ),
  );

  it.effect("generates commit message with branch when includeBranch is true", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          subject: "Add important change",
          body: "",
          branch: "fix/important-system-change",
        }),
        stdinMustContain: "branch must be a short semantic git branch fragment",
      },
      Effect.gen(function* () {
        const textGeneration = yield* TextGeneration;

        const generated = yield* textGeneration.generateCommitMessage({
          cwd: process.cwd(),
          branch: "feature/codex-effect",
          stagedSummary: "M README.md",
          stagedPatch: "diff --git a/README.md b/README.md",
          includeBranch: true,
          modelSelection: DEFAULT_TEST_MODEL_SELECTION,
        });

        expect(generated.subject).toBe("Add important change");
        expect(generated.branch).toBe("feature/fix/important-system-change");
      }),
    ),
  );

  it.effect("generates PR content and trims markdown body", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          title: "  Improve orchestration flow\nwith ignored suffix",
          body: "\n## Summary\n- improve flow\n\n## Testing\n- bun test\n\n",
        }),
      },
      Effect.gen(function* () {
        const textGeneration = yield* TextGeneration;

        const generated = yield* textGeneration.generatePrContent({
          cwd: process.cwd(),
          baseBranch: "main",
          headBranch: "feature/codex-effect",
          commitSummary: "feat: improve orchestration flow",
          diffSummary: "2 files changed",
          diffPatch: "diff --git a/a.ts b/a.ts",
          modelSelection: DEFAULT_TEST_MODEL_SELECTION,
        });

        expect(generated.title).toBe("Improve orchestration flow");
        expect(generated.body.startsWith("## Summary")).toBe(true);
        expect(generated.body.endsWith("\n\n")).toBe(false);
      }),
    ),
  );

  it.effect("generates branch names and normalizes branch fragments", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          branch: "  Feat/Session  ",
        }),
        stdinMustNotContain: "Image attachments supplied to the model",
      },
      Effect.gen(function* () {
        const textGeneration = yield* TextGeneration;

        const generated = yield* textGeneration.generateBranchName({
          cwd: process.cwd(),
          message: "Please update session handling.",
          modelSelection: DEFAULT_TEST_MODEL_SELECTION,
        });

        expect(generated.branch).toBe("feat/session");
      }),
    ),
  );

  it.effect("generates thread titles and trims them for sidebar use", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          title:
            '  "Investigate websocket reconnect regressions after worktree restore"  \nignored line',
        }),
      },
      Effect.gen(function* () {
        const textGeneration = yield* TextGeneration;

        const generated = yield* textGeneration.generateThreadTitle({
          cwd: process.cwd(),
          message: "Please investigate websocket reconnect regressions after a worktree restore.",
          modelSelection: DEFAULT_TEST_MODEL_SELECTION,
        });

        expect(generated.title).toBe("Investigate websocket reconnect regressions aft...");
      }),
    ),
  );

  it.effect("falls back when thread title normalization becomes whitespace-only", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          title: '  """   """  ',
        }),
      },
      Effect.gen(function* () {
        const textGeneration = yield* TextGeneration;

        const generated = yield* textGeneration.generateThreadTitle({
          cwd: process.cwd(),
          message: "Name this thread.",
          modelSelection: DEFAULT_TEST_MODEL_SELECTION,
        });

        expect(generated.title).toBe("New thread");
      }),
    ),
  );

  it.effect("trims whitespace exposed after quote removal in thread titles", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          title: `  "' hello world '"  `,
        }),
      },
      Effect.gen(function* () {
        const textGeneration = yield* TextGeneration;

        const generated = yield* textGeneration.generateThreadTitle({
          cwd: process.cwd(),
          message: "Name this thread.",
          modelSelection: DEFAULT_TEST_MODEL_SELECTION,
        });

        expect(generated.title).toBe("hello world");
      }),
    ),
  );

  it.effect("omits attachment metadata section when no attachments are provided", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          branch: "fix/session-timeout",
        }),
        stdinMustNotContain: "Attachment metadata:",
      },
      Effect.gen(function* () {
        const textGeneration = yield* TextGeneration;

        const generated = yield* textGeneration.generateBranchName({
          cwd: process.cwd(),
          message: "Fix timeout behavior.",
          modelSelection: DEFAULT_TEST_MODEL_SELECTION,
        });

        expect(generated.branch).toBe("fix/session-timeout");
      }),
    ),
  );

  it.effect("passes image attachments through as codex image inputs", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          branch: "fix/ui-regression",
        }),
        requireImage: true,
        stdinMustContain: "Attachment metadata:",
      },
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const { attachmentsDir } = yield* ServerConfig;
        const attachmentId = `thread-branch-image-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
        const attachmentPath = path.join(attachmentsDir, `${attachmentId}.png`);
        yield* fs.makeDirectory(attachmentsDir, { recursive: true });
        yield* fs.writeFile(attachmentPath, Buffer.from("hello"));

        const textGeneration = yield* TextGeneration;
        const generated = yield* textGeneration.generateBranchName({
          modelSelection: DEFAULT_TEST_MODEL_SELECTION,
          cwd: process.cwd(),
          message: "Fix layout bug from screenshot.",
          attachments: [
            {
              type: "image",
              id: attachmentId,
              name: "bug.png",
              mimeType: "image/png",
              sizeBytes: 5,
            },
          ],
        });

        expect(generated.branch).toBe("fix/ui-regression");
      }),
    ),
  );

  it.effect("resolves persisted attachment ids to files for codex image inputs", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          branch: "fix/ui-regression",
        }),
        requireImage: true,
      },
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const { attachmentsDir } = yield* ServerConfig;
        const attachmentId = `thread-1-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
        const imagePath = path.join(attachmentsDir, `${attachmentId}.png`);
        yield* fs.makeDirectory(attachmentsDir, { recursive: true });
        yield* fs.writeFile(imagePath, Buffer.from("hello"));

        const textGeneration = yield* TextGeneration;
        const generated = yield* textGeneration
          .generateBranchName({
            modelSelection: DEFAULT_TEST_MODEL_SELECTION,
            cwd: process.cwd(),
            message: "Fix layout bug from screenshot.",
            attachments: [
              {
                type: "image",
                id: attachmentId,
                name: "bug.png",
                mimeType: "image/png",
                sizeBytes: 5,
              },
            ],
          })
          .pipe(
            Effect.tap(() =>
              fs.stat(imagePath).pipe(
                Effect.map((fileInfo) => {
                  expect(fileInfo.type).toBe("File");
                }),
              ),
            ),
            Effect.ensuring(fs.remove(imagePath).pipe(Effect.catch(() => Effect.void))),
          );

        expect(generated.branch).toBe("fix/ui-regression");
      }),
    ),
  );

  it.effect("ignores missing attachment ids for codex image inputs", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({
          branch: "fix/ui-regression",
        }),
        requireImage: true,
      },
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const { attachmentsDir } = yield* ServerConfig;
        const missingAttachmentId = `thread-missing-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
        const missingPath = path.join(attachmentsDir, `${missingAttachmentId}.png`);
        yield* fs.remove(missingPath).pipe(Effect.catch(() => Effect.void));

        const textGeneration = yield* TextGeneration;
        const result = yield* textGeneration
          .generateBranchName({
            modelSelection: DEFAULT_TEST_MODEL_SELECTION,
            cwd: process.cwd(),
            message: "Fix layout bug from screenshot.",
            attachments: [
              {
                type: "image",
                id: missingAttachmentId,
                name: "outside.png",
                mimeType: "image/png",
                sizeBytes: 5,
              },
            ],
          })
          .pipe(Effect.result);

        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result)) {
          expect(result.failure).toBeInstanceOf(TextGenerationError);
          expect(result.failure.message).toContain("missing --image input");
        }
      }),
    ),
  );

  it.effect(
    "fails with typed TextGenerationError when codex returns wrong branch payload shape",
    () =>
      withFakeCodexEnv(
        {
          output: JSON.stringify({
            title: "This is not a branch payload",
          }),
        },
        Effect.gen(function* () {
          const textGeneration = yield* TextGeneration;

          const result = yield* textGeneration
            .generateBranchName({
              cwd: process.cwd(),
              message: "Fix websocket reconnect flake",
              modelSelection: DEFAULT_TEST_MODEL_SELECTION,
            })
            .pipe(Effect.result);

          expect(Result.isFailure(result)).toBe(true);
          if (Result.isFailure(result)) {
            expect(result.failure).toBeInstanceOf(TextGenerationError);
            expect(result.failure.message).toContain("Codex returned invalid structured output");
          }
        }),
      ),
  );

  it.effect("returns typed TextGenerationError when codex exits non-zero", () =>
    withFakeCodexEnv(
      {
        output: JSON.stringify({ subject: "ignored", body: "" }),
        exitCode: 1,
        stderr: "codex execution failed",
      },
      Effect.gen(function* () {
        const textGeneration = yield* TextGeneration;

        const result = yield* textGeneration
          .generateCommitMessage({
            cwd: process.cwd(),
            branch: "feature/codex-error",
            stagedSummary: "M README.md",
            stagedPatch: "diff --git a/README.md b/README.md",
            modelSelection: DEFAULT_TEST_MODEL_SELECTION,
          })
          .pipe(Effect.result);

        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result)) {
          expect(result.failure).toBeInstanceOf(TextGenerationError);
          expect(result.failure.message).toContain(
            "Codex CLI command failed: codex execution failed",
          );
        }
      }),
    ),
  );
});
