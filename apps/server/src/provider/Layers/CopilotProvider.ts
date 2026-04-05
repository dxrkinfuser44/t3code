import {
  ServerSettingsError,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderAuth,
  type ServerProviderModel,
  type ServerProviderState,
} from "@t3tools/contracts";
import { Effect, Equal, Layer, Option, Result, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  buildServerProvider,
  DEFAULT_TIMEOUT_MS,
  detailFromResult,
  extractAuthBoolean,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type CommandResult,
} from "../providerSnapshot";
import { makeManagedServerProvider } from "../makeManagedServerProvider";
import { getCodexModelCapabilities } from "./CodexProvider";
import { CopilotProvider } from "../Services/CopilotProvider";
import { ServerSettingsService } from "../../serverSettings";

const PROVIDER = "copilot" as const;

interface CopilotProviderSettings {
  readonly enabled: boolean;
  readonly binaryPath: string;
  readonly customModels: ReadonlyArray<string>;
}

const DEFAULT_COPILOT_PROVIDER_SETTINGS: CopilotProviderSettings = {
  enabled: true,
  binaryPath: "copilot",
  customModels: [],
};

const COPILOT_BUILT_IN_MODEL_DEFS = [
  { slug: "gpt-5.4", name: "GPT-5.4" },
  { slug: "gpt-5.4-mini", name: "GPT-5.4 Mini" },
  { slug: "gpt-5.3-codex", name: "GPT-5.3 Codex" },
  { slug: "gpt-5.3-codex-spark", name: "GPT-5.3 Codex Spark" },
  { slug: "gpt-5.2-codex", name: "GPT-5.2 Codex" },
  { slug: "gpt-5.2", name: "GPT-5.2" },
] as const;

const BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = COPILOT_BUILT_IN_MODEL_DEFS.map(
  (model) => ({
    slug: model.slug,
    name: model.name,
    isCustom: false,
    capabilities: getCodexModelCapabilities(model.slug),
  }),
);

function asRecord(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object" || globalThis.Array.isArray(value)) {
    return undefined;
  }
  return value as Record<string, unknown>;
}

function asBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function asNonEmptyString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function asStringArray(value: unknown): ReadonlyArray<string> {
  if (!globalThis.Array.isArray(value)) {
    return [];
  }

  const normalized: string[] = [];
  for (const candidate of value) {
    const trimmed = asNonEmptyString(candidate);
    if (!trimmed) {
      continue;
    }
    normalized.push(trimmed);
  }
  return normalized;
}

function readCopilotProviderSettings(settings: unknown): CopilotProviderSettings {
  const root = asRecord(settings);
  const providers = asRecord(root?.providers);
  const copilot = asRecord(providers?.copilot);

  if (!copilot) {
    return DEFAULT_COPILOT_PROVIDER_SETTINGS;
  }

  return {
    enabled: asBoolean(copilot.enabled) ?? DEFAULT_COPILOT_PROVIDER_SETTINGS.enabled,
    binaryPath:
      asNonEmptyString(copilot.binaryPath) ?? DEFAULT_COPILOT_PROVIDER_SETTINGS.binaryPath,
    customModels: asStringArray(copilot.customModels),
  };
}

export function getCopilotModelCapabilities(model: string | null | undefined): ModelCapabilities {
  const slug = model?.trim();
  return (
    BUILT_IN_MODELS.find((candidate) => candidate.slug === slug)?.capabilities ?? {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    }
  );
}

export function parseCopilotAuthStatusFromOutput(result: CommandResult): {
  readonly status: Exclude<ServerProviderState, "disabled">;
  readonly auth: Pick<ServerProviderAuth, "status">;
  readonly message?: string;
} {
  const lowerOutput = `${result.stdout}\n${result.stderr}`.toLowerCase();

  if (
    lowerOutput.includes("unknown command") ||
    lowerOutput.includes("unrecognized command") ||
    lowerOutput.includes("unexpected argument")
  ) {
    return {
      status: "warning",
      auth: { status: "unknown" },
      message:
        "GitHub Copilot authentication status command is unavailable in this version of Copilot CLI.",
    };
  }

  if (
    lowerOutput.includes("not logged in") ||
    lowerOutput.includes("login required") ||
    lowerOutput.includes("authentication required") ||
    lowerOutput.includes("run `copilot auth login`") ||
    lowerOutput.includes("run copilot auth login")
  ) {
    return {
      status: "error",
      auth: { status: "unauthenticated" },
      message: "GitHub Copilot CLI is not authenticated. Run `copilot auth login` and try again.",
    };
  }

  const parsedAuth = (() => {
    const trimmed = result.stdout.trim();
    if (!trimmed || (!trimmed.startsWith("{") && !trimmed.startsWith("["))) {
      return { attemptedJsonParse: false as const, auth: undefined as boolean | undefined };
    }
    try {
      return {
        attemptedJsonParse: true as const,
        auth: extractAuthBoolean(JSON.parse(trimmed)),
      };
    } catch {
      return { attemptedJsonParse: false as const, auth: undefined as boolean | undefined };
    }
  })();

  if (parsedAuth.auth === true) {
    return { status: "ready", auth: { status: "authenticated" } };
  }
  if (parsedAuth.auth === false) {
    return {
      status: "error",
      auth: { status: "unauthenticated" },
      message: "GitHub Copilot CLI is not authenticated. Run `copilot auth login` and try again.",
    };
  }
  if (parsedAuth.attemptedJsonParse) {
    return {
      status: "warning",
      auth: { status: "unknown" },
      message:
        "Could not verify GitHub Copilot authentication status from JSON output (missing auth marker).",
    };
  }
  if (result.code === 0) {
    return { status: "ready", auth: { status: "authenticated" } };
  }

  const detail = detailFromResult(result);
  return {
    status: "warning",
    auth: { status: "unknown" },
    message: detail
      ? `Could not verify GitHub Copilot authentication status. ${detail}`
      : "Could not verify GitHub Copilot authentication status.",
  };
}

const runCopilotCommand = Effect.fn("runCopilotCommand")(function* (args: ReadonlyArray<string>) {
  const settingsService = yield* ServerSettingsService;
  const copilotSettings = yield* settingsService.getSettings.pipe(
    Effect.map(readCopilotProviderSettings),
  );

  const command = ChildProcess.make(copilotSettings.binaryPath, [...args], {
    shell: process.platform === "win32",
  });

  return yield* spawnAndCollect(copilotSettings.binaryPath, command);
});

export const checkCopilotProviderStatus = Effect.fn("checkCopilotProviderStatus")(
  function* (): Effect.fn.Return<
    ServerProvider,
    ServerSettingsError,
    ChildProcessSpawner.ChildProcessSpawner | ServerSettingsService
  > {
    const settingsService = yield* ServerSettingsService;
    const copilotSettings = yield* settingsService.getSettings.pipe(
      Effect.map(readCopilotProviderSettings),
    );
    const checkedAt = new Date().toISOString();
    const models = providerModelsFromSettings(
      BUILT_IN_MODELS,
      PROVIDER,
      copilotSettings.customModels,
    );

    if (!copilotSettings.enabled) {
      return buildServerProvider({
        provider: PROVIDER,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "GitHub Copilot is disabled in T3 Code settings.",
        },
      });
    }

    const versionProbe = yield* runCopilotCommand(["--version"]).pipe(
      Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
      Effect.result,
    );

    if (Result.isFailure(versionProbe)) {
      const error = versionProbe.failure;
      return buildServerProvider({
        provider: PROVIDER,
        enabled: copilotSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: !isCommandMissingCause(error),
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: isCommandMissingCause(error)
            ? "GitHub Copilot CLI (`copilot`) is not installed or not on PATH."
            : `Failed to execute GitHub Copilot CLI health check: ${error instanceof Error ? error.message : String(error)}.`,
        },
      });
    }

    if (Option.isNone(versionProbe.success)) {
      return buildServerProvider({
        provider: PROVIDER,
        enabled: copilotSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message:
            "GitHub Copilot CLI is installed but failed to run. Timed out while running command.",
        },
      });
    }

    const version = versionProbe.success.value;
    const parsedVersion = parseGenericCliVersion(`${version.stdout}\n${version.stderr}`);
    if (version.code !== 0) {
      const detail = detailFromResult(version);
      return buildServerProvider({
        provider: PROVIDER,
        enabled: copilotSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: parsedVersion,
          status: "error",
          auth: { status: "unknown" },
          message: detail
            ? `GitHub Copilot CLI is installed but failed to run. ${detail}`
            : "GitHub Copilot CLI is installed but failed to run.",
        },
      });
    }

    const authProbe = yield* runCopilotCommand(["auth", "status"]).pipe(
      Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
      Effect.result,
    );

    if (Result.isFailure(authProbe)) {
      const error = authProbe.failure;
      return buildServerProvider({
        provider: PROVIDER,
        enabled: copilotSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: parsedVersion,
          status: "warning",
          auth: { status: "unknown" },
          message:
            error instanceof Error
              ? `Could not verify GitHub Copilot authentication status: ${error.message}.`
              : "Could not verify GitHub Copilot authentication status.",
        },
      });
    }

    if (Option.isNone(authProbe.success)) {
      return buildServerProvider({
        provider: PROVIDER,
        enabled: copilotSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: parsedVersion,
          status: "warning",
          auth: { status: "unknown" },
          message:
            "Could not verify GitHub Copilot authentication status. Timed out while running command.",
        },
      });
    }

    const parsed = parseCopilotAuthStatusFromOutput(authProbe.success.value);
    return buildServerProvider({
      provider: PROVIDER,
      enabled: copilotSettings.enabled,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: parsedVersion,
        status: parsed.status,
        auth: parsed.auth,
        ...(parsed.message ? { message: parsed.message } : {}),
      },
    });
  },
);

export const CopilotProviderLive = Layer.effect(
  CopilotProvider,
  Effect.gen(function* () {
    const serverSettings = yield* ServerSettingsService;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const checkProvider = checkCopilotProviderStatus().pipe(
      Effect.provideService(ServerSettingsService, serverSettings),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );

    return yield* makeManagedServerProvider<CopilotProviderSettings>({
      getSettings: serverSettings.getSettings.pipe(
        Effect.map(readCopilotProviderSettings),
        Effect.orDie,
      ),
      streamSettings: serverSettings.streamChanges.pipe(Stream.map(readCopilotProviderSettings)),
      haveSettingsChanged: (previous: CopilotProviderSettings, next: CopilotProviderSettings) =>
        !Equal.equals(previous, next),
      checkProvider,
    });
  }),
);
