/**
 * CopilotAdapterLive - Scoped live implementation for the Copilot provider adapter.
 *
 * Wraps `@github/copilot-sdk` sessions behind the generic provider adapter contract
 * and emits canonical runtime events.
 *
 * @module CopilotAdapterLive
 */
import {
  ApprovalRequestId,
  type CanonicalItemType,
  type CanonicalRequestType,
  EventId,
  type ProviderApprovalDecision,
  ProviderItemId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  type ThreadTokenUsageSnapshot,
  TurnId,
} from "@t3tools/contracts";
import { Effect, FileSystem, Layer, Queue, Stream } from "effect";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";
import { CopilotAdapter, type CopilotAdapterShape } from "../Services/CopilotAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = "copilot" as const;
const DEFAULT_COPILOT_MODELS = ["gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex", "gpt-5.3-codex-spark"];
const DEFAULT_ATTACHMENT_PROMPT = "Please review the attached files and summarize the result.";

type PermissionResultKind =
  | "approved"
  | "denied-by-rules"
  | "denied-no-approval-rule-and-could-not-request-from-user"
  | "denied-interactively-by-user"
  | "denied-by-content-exclusion-policy"
  | "denied-by-permission-request-hook";

interface CopilotClientLike {
  start(): Promise<void>;
  stop(): Promise<unknown>;
  forceStop?(): Promise<void>;
  createSession(config: Record<string, unknown>): Promise<CopilotSessionLike>;
  resumeSession(sessionId: string, config: Record<string, unknown>): Promise<CopilotSessionLike>;
  listModels?(): Promise<ReadonlyArray<unknown>>;
}

interface CopilotSessionLike {
  readonly sessionId: string;
  send(options: {
    readonly prompt: string;
    readonly attachments?: ReadonlyArray<{
      readonly type: string;
      readonly path?: string;
      readonly displayName?: string;
    }>;
    readonly mode?: "enqueue" | "immediate";
  }): Promise<string>;
  on(handler: (event: CopilotSessionEvent) => void): (() => void) | void;
  abort(): Promise<void>;
  disconnect(): Promise<void>;
  setModel?(model: string, options?: { reasoningEffort?: string }): Promise<void>;
}

interface CopilotSessionEvent {
  readonly type: string;
  readonly id?: string;
  readonly timestamp?: string;
  readonly data?: Record<string, unknown>;
  readonly [key: string]: unknown;
}

interface PendingPermission {
  readonly requestId: ApprovalRequestId;
  readonly requestType: CanonicalRequestType;
  readonly detail?: string;
  readonly toolCallId?: string;
  readonly decisionPromise: Promise<ProviderApprovalDecision>;
  readonly resolveDecision: (decision: ProviderApprovalDecision) => void;
  consumedByHandler: boolean;
}

interface PendingUserInput {
  readonly requestId: ApprovalRequestId;
  readonly question: string;
  readonly choices: ReadonlyArray<string>;
  readonly allowFreeform: boolean;
  readonly toolCallId?: string;
  readonly answersPromise: Promise<ProviderUserInputAnswers>;
  readonly resolveAnswers: (answers: ProviderUserInputAnswers) => void;
  consumedByHandler: boolean;
}

interface CopilotTurnState {
  readonly id: TurnId;
  readonly items: Array<unknown>;
}

interface AssistantMessageState {
  readonly itemId: ProviderItemId;
  sawDelta: boolean;
}

interface CopilotSessionContext {
  session: ProviderSession;
  readonly sdkSession: CopilotSessionLike;
  readonly pendingPermissions: Map<ApprovalRequestId, PendingPermission>;
  readonly permissionRequestIdByToolCallId: Map<string, ApprovalRequestId>;
  readonly pendingPermissionOrder: Array<ApprovalRequestId>;
  readonly pendingUserInputs: Map<ApprovalRequestId, PendingUserInput>;
  readonly userInputRequestIdByToolCallId: Map<string, ApprovalRequestId>;
  readonly pendingUserInputOrder: Array<ApprovalRequestId>;
  readonly turns: Array<CopilotTurnState>;
  readonly turnByMessageId: Map<string, TurnId>;
  readonly assistantMessageStateByMessageId: Map<string, AssistantMessageState>;
  currentTurnId: TurnId | undefined;
  stopped: boolean;
  unsubscribe: (() => void) | undefined;
}

export interface CopilotAdapterLiveOptions {
  readonly createClient?: (input: { readonly binaryPath: string }) => Promise<CopilotClientLike>;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
}

function toMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error && cause.message.length > 0) {
    return cause.message;
  }
  return fallback;
}

function asObject(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function asBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function asStringArray(value: unknown): ReadonlyArray<string> {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry): entry is string => typeof entry === "string");
}

function nowIso(): string {
  return new Date().toISOString();
}

function toSessionError(
  threadId: ThreadId,
  cause: unknown,
): ProviderAdapterSessionNotFoundError | ProviderAdapterSessionClosedError | undefined {
  const normalized = toMessage(cause, "").toLowerCase();
  if (normalized.includes("unknown session") || normalized.includes("session not found")) {
    return new ProviderAdapterSessionNotFoundError({
      provider: PROVIDER,
      threadId,
      cause,
    });
  }
  if (normalized.includes("session closed") || normalized.includes("already disconnected")) {
    return new ProviderAdapterSessionClosedError({
      provider: PROVIDER,
      threadId,
      cause,
    });
  }
  return undefined;
}

function toRequestError(threadId: ThreadId, method: string, cause: unknown): ProviderAdapterError {
  const sessionError = toSessionError(threadId, cause);
  if (sessionError) {
    return sessionError;
  }
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: toMessage(cause, `${method} failed`),
    cause,
  });
}

function asRuntimeItemId(value: ProviderItemId): RuntimeItemId {
  return RuntimeItemId.makeUnsafe(value);
}

function asRuntimeRequestId(value: ApprovalRequestId): RuntimeRequestId {
  return RuntimeRequestId.makeUnsafe(value);
}

function toTurnId(value: unknown): TurnId | undefined {
  const candidate = asString(value)?.trim();
  return candidate ? TurnId.makeUnsafe(candidate) : undefined;
}

function toApprovalRequestId(value: unknown): ApprovalRequestId | undefined {
  const candidate = asString(value)?.trim();
  return candidate ? ApprovalRequestId.makeUnsafe(candidate) : undefined;
}

function toProviderItemId(value: unknown): ProviderItemId | undefined {
  const candidate = asString(value)?.trim();
  return candidate ? ProviderItemId.makeUnsafe(candidate) : undefined;
}

function toCanonicalRequestType(kind: string | undefined): CanonicalRequestType {
  switch (kind) {
    case "shell":
      return "command_execution_approval";
    case "write":
      return "file_change_approval";
    case "read":
      return "file_read_approval";
    case "mcp":
    case "custom-tool":
    case "url":
    case "memory":
    case "hook":
      return "dynamic_tool_call";
    default:
      return "unknown";
  }
}

function decisionToPermissionResultKind(decision: ProviderApprovalDecision): PermissionResultKind {
  switch (decision) {
    case "accept":
    case "acceptForSession":
      return "approved";
    case "cancel":
    case "decline":
      return "denied-interactively-by-user";
    default:
      return "denied-no-approval-rule-and-could-not-request-from-user";
  }
}

function permissionResultKindToDecision(
  resultKind: string | undefined,
): ProviderApprovalDecision | undefined {
  switch (resultKind) {
    case "approved":
      return "accept";
    case "denied-interactively-by-user":
      return "decline";
    case "denied-by-rules":
    case "denied-no-approval-rule-and-could-not-request-from-user":
    case "denied-by-content-exclusion-policy":
    case "denied-by-permission-request-hook":
      return "decline";
    default:
      return undefined;
  }
}

function normalizeToolItemType(toolName: string | undefined): CanonicalItemType {
  const normalized = toolName?.toLowerCase().trim() ?? "";
  if (
    normalized.includes("bash") ||
    normalized.includes("shell") ||
    normalized.includes("command") ||
    normalized.includes("terminal")
  ) {
    return "command_execution";
  }
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("file") ||
    normalized.includes("patch") ||
    normalized.includes("replace")
  ) {
    return "file_change";
  }
  if (normalized.includes("mcp")) {
    return "mcp_tool_call";
  }
  if (normalized.includes("web") || normalized.includes("url")) {
    return "web_search";
  }
  if (normalized.includes("image")) {
    return "image_view";
  }
  return "dynamic_tool_call";
}

function itemTitle(itemType: CanonicalItemType): string | undefined {
  switch (itemType) {
    case "assistant_message":
      return "Assistant message";
    case "reasoning":
      return "Reasoning";
    case "command_execution":
      return "Command run";
    case "file_change":
      return "File change";
    case "mcp_tool_call":
      return "MCP tool call";
    case "web_search":
      return "Web search";
    case "image_view":
      return "Image view";
    case "dynamic_tool_call":
      return "Tool call";
    default:
      return undefined;
  }
}

function summarizePermissionRequest(permission: Record<string, unknown>): string | undefined {
  const kind = asString(permission.kind);
  switch (kind) {
    case "shell": {
      const command = asString(permission.fullCommandText)?.trim();
      return command ? `Run shell command: ${command}` : "Run shell command";
    }
    case "write": {
      const path = asString(permission.fileName) ?? asString(permission.path);
      return path ? `Write file: ${path}` : "Write file";
    }
    case "read": {
      const path = asString(permission.path);
      return path ? `Read file: ${path}` : "Read file";
    }
    case "url": {
      const url = asString(permission.url);
      return url ? `Fetch URL: ${url}` : "Fetch URL";
    }
    case "mcp": {
      const toolName = asString(permission.toolName) ?? asString(permission.toolTitle);
      return toolName ? `MCP tool: ${toolName}` : "MCP tool";
    }
    case "custom-tool": {
      const toolName = asString(permission.toolName);
      return toolName ? `Custom tool: ${toolName}` : "Custom tool";
    }
    case "memory": {
      const subject = asString(permission.subject);
      return subject ? `Memory operation: ${subject}` : "Memory operation";
    }
    case "hook": {
      const toolName = asString(permission.toolName);
      return toolName ? `Hook confirmation for: ${toolName}` : "Hook confirmation";
    }
    default:
      return undefined;
  }
}

function normalizeUsageFromSessionUsageInfo(
  data: Record<string, unknown>,
): ThreadTokenUsageSnapshot | undefined {
  const usedTokens = asNumber(data.currentTokens);
  const maxTokens = asNumber(data.tokenLimit);
  if (usedTokens === undefined || usedTokens < 0) {
    return undefined;
  }

  const systemTokens = asNumber(data.systemTokens) ?? 0;
  const conversationTokens = asNumber(data.conversationTokens) ?? 0;
  const toolDefinitionTokens = asNumber(data.toolDefinitionsTokens) ?? 0;
  const inputTokens = systemTokens + conversationTokens + toolDefinitionTokens;

  const outputTokens = asNumber(data.outputTokens);

  return {
    usedTokens,
    ...(maxTokens !== undefined && maxTokens > 0 ? { maxTokens } : {}),
    ...(inputTokens > 0 ? { inputTokens } : {}),
    ...(outputTokens !== undefined && outputTokens >= 0 ? { outputTokens } : {}),
    lastUsedTokens: usedTokens,
    ...(inputTokens > 0 ? { lastInputTokens: inputTokens } : {}),
    ...(outputTokens !== undefined && outputTokens >= 0 ? { lastOutputTokens: outputTokens } : {}),
  };
}

function answerFromUserInputAnswers(answers: ProviderUserInputAnswers): string | undefined {
  for (const value of Object.values(answers)) {
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed.length > 0) {
        return trimmed;
      }
      continue;
    }

    if (Array.isArray(value)) {
      for (const entry of value) {
        if (typeof entry === "string") {
          const trimmed = entry.trim();
          if (trimmed.length > 0) {
            return trimmed;
          }
        }
      }
      continue;
    }

    const objectValue = asObject(value);
    if (!objectValue) {
      continue;
    }

    const nestedAnswers = asStringArray(objectValue.answers);
    for (const entry of nestedAnswers) {
      const trimmed = entry.trim();
      if (trimmed.length > 0) {
        return trimmed;
      }
    }
  }

  return undefined;
}

function toUserInputOptions(
  choices: ReadonlyArray<string>,
): Array<{ label: string; description: string }> {
  if (choices.length > 0) {
    return choices.map((choice) => ({
      label: choice,
      description: choice,
    }));
  }

  return [
    {
      label: "Provide answer",
      description: "Enter a freeform response",
    },
    {
      label: "Cancel",
      description: "Decline this request",
    },
  ];
}

function createPendingPermission(input: {
  readonly requestId: ApprovalRequestId;
  readonly requestType: CanonicalRequestType;
  readonly detail?: string;
  readonly toolCallId?: string;
}): PendingPermission {
  let resolveDecision: ((decision: ProviderApprovalDecision) => void) | undefined;
  const decisionPromise = new Promise<ProviderApprovalDecision>((resolve) => {
    resolveDecision = resolve;
  });

  return {
    requestId: input.requestId,
    requestType: input.requestType,
    ...(input.detail ? { detail: input.detail } : {}),
    ...(input.toolCallId ? { toolCallId: input.toolCallId } : {}),
    decisionPromise,
    resolveDecision: (decision) => {
      resolveDecision?.(decision);
      resolveDecision = undefined;
    },
    consumedByHandler: false,
  };
}

function createPendingUserInput(input: {
  readonly requestId: ApprovalRequestId;
  readonly question: string;
  readonly choices: ReadonlyArray<string>;
  readonly allowFreeform: boolean;
  readonly toolCallId?: string;
}): PendingUserInput {
  let resolveAnswers: ((answers: ProviderUserInputAnswers) => void) | undefined;
  const answersPromise = new Promise<ProviderUserInputAnswers>((resolve) => {
    resolveAnswers = resolve;
  });

  return {
    requestId: input.requestId,
    question: input.question,
    choices: input.choices,
    allowFreeform: input.allowFreeform,
    ...(input.toolCallId ? { toolCallId: input.toolCallId } : {}),
    answersPromise,
    resolveAnswers: (answers) => {
      resolveAnswers?.(answers);
      resolveAnswers = undefined;
    },
    consumedByHandler: false,
  };
}

function removeFromOrder(list: Array<ApprovalRequestId>, requestId: ApprovalRequestId): void {
  const index = list.indexOf(requestId);
  if (index >= 0) {
    list.splice(index, 1);
  }
}

function selectPendingPermission(
  context: CopilotSessionContext,
  request: Record<string, unknown>,
): PendingPermission | undefined {
  const directRequestId = toApprovalRequestId(request.requestId);
  if (directRequestId) {
    const pending = context.pendingPermissions.get(directRequestId);
    if (pending && !pending.consumedByHandler) {
      pending.consumedByHandler = true;
      return pending;
    }
  }

  const toolCallId = asString(request.toolCallId);
  if (toolCallId) {
    const mappedRequestId = context.permissionRequestIdByToolCallId.get(toolCallId);
    if (mappedRequestId) {
      const pending = context.pendingPermissions.get(mappedRequestId);
      if (pending && !pending.consumedByHandler) {
        pending.consumedByHandler = true;
        return pending;
      }
    }
  }

  for (const requestId of context.pendingPermissionOrder) {
    const pending = context.pendingPermissions.get(requestId);
    if (!pending || pending.consumedByHandler) {
      continue;
    }
    pending.consumedByHandler = true;
    return pending;
  }

  return undefined;
}

function selectPendingUserInput(
  context: CopilotSessionContext,
  request: Record<string, unknown>,
): PendingUserInput | undefined {
  const directRequestId = toApprovalRequestId(request.requestId);
  if (directRequestId) {
    const pending = context.pendingUserInputs.get(directRequestId);
    if (pending && !pending.consumedByHandler) {
      pending.consumedByHandler = true;
      return pending;
    }
  }

  const toolCallId = asString(request.toolCallId);
  if (toolCallId) {
    const mappedRequestId = context.userInputRequestIdByToolCallId.get(toolCallId);
    if (mappedRequestId) {
      const pending = context.pendingUserInputs.get(mappedRequestId);
      if (pending && !pending.consumedByHandler) {
        pending.consumedByHandler = true;
        return pending;
      }
    }
  }

  for (const requestId of context.pendingUserInputOrder) {
    const pending = context.pendingUserInputs.get(requestId);
    if (!pending || pending.consumedByHandler) {
      continue;
    }
    pending.consumedByHandler = true;
    return pending;
  }

  return undefined;
}

const makeCopilotAdapter = Effect.fn("makeCopilotAdapter")(function* (
  options?: CopilotAdapterLiveOptions,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const serverConfig = yield* ServerConfig;
  const serverSettingsService = yield* ServerSettingsService;
  const nativeEventLogger =
    options?.nativeEventLogger ??
    (options?.nativeEventLogPath !== undefined
      ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, {
          stream: "native",
        })
      : undefined);

  const sessions = new Map<ThreadId, CopilotSessionContext>();
  const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  let sdkClient: CopilotClientLike | undefined;
  let modelCatalogCache: ReadonlyArray<string> | undefined;

  const logNativeEvent = Effect.fn("logNativeEvent")(function* (
    threadId: ThreadId,
    event: unknown,
  ) {
    if (!nativeEventLogger) {
      return;
    }
    yield* nativeEventLogger.write(
      {
        observedAt: nowIso(),
        provider: PROVIDER,
        threadId,
        event,
      },
      threadId,
    );
  });

  const ensureClient = Effect.fn("ensureClient")(function* (threadId: ThreadId) {
    if (sdkClient) {
      return sdkClient;
    }

    const copilotSettings = yield* serverSettingsService.getSettings.pipe(
      Effect.map((settings) => settings.providers.copilot),
      Effect.mapError(
        (cause) =>
          new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId,
            detail: "Failed to load Copilot settings.",
            cause,
          }),
      ),
    );

    const client = yield* Effect.tryPromise({
      try: async () => {
        if (options?.createClient) {
          return await options.createClient({
            binaryPath: copilotSettings.binaryPath,
          });
        }

        const sdk = await import("@github/copilot-sdk");
        return new sdk.CopilotClient(
          copilotSettings.binaryPath ? { cliPath: copilotSettings.binaryPath } : {},
        ) as unknown as CopilotClientLike;
      },
      catch: (cause) =>
        new ProviderAdapterProcessError({
          provider: PROVIDER,
          threadId,
          detail:
            "Could not load @github/copilot-sdk. Install dependencies and ensure the Copilot SDK is available.",
          cause,
        }),
    });

    yield* Effect.tryPromise({
      try: () => client.start(),
      catch: (cause) =>
        new ProviderAdapterProcessError({
          provider: PROVIDER,
          threadId,
          detail: toMessage(cause, "Failed to start Copilot SDK client."),
          cause,
        }),
    });

    sdkClient = client;
    modelCatalogCache = undefined;
    return client;
  });

  const stopClient = Effect.fn("stopClient")(function* () {
    if (!sdkClient) {
      return;
    }

    const client = sdkClient;
    sdkClient = undefined;
    modelCatalogCache = undefined;

    const stopped = yield* Effect.tryPromise({
      try: () => client.stop(),
      catch: () => undefined,
    }).pipe(Effect.orElseSucceed(() => undefined));

    if (stopped === undefined && typeof client.forceStop === "function") {
      yield* Effect.tryPromise({
        try: () => client.forceStop!(),
        catch: () => undefined,
      }).pipe(Effect.orElseSucceed(() => undefined));
    }
  });

  const listModelsWithFallback = Effect.fn("listModelsWithFallback")(function* (
    threadId: ThreadId,
  ) {
    if (modelCatalogCache && modelCatalogCache.length > 0) {
      return modelCatalogCache;
    }

    const models = yield* ensureClient(threadId).pipe(
      Effect.flatMap((client) =>
        Effect.tryPromise({
          try: async () => {
            if (typeof client.listModels !== "function") {
              return DEFAULT_COPILOT_MODELS;
            }
            const listed = await client.listModels();
            const modelIds = listed
              .map((entry) => {
                const record = asObject(entry);
                return asString(record?.id)?.trim();
              })
              .filter(
                (modelId): modelId is string => typeof modelId === "string" && modelId.length > 0,
              );
            return modelIds.length > 0 ? modelIds : DEFAULT_COPILOT_MODELS;
          },
          catch: () => DEFAULT_COPILOT_MODELS,
        }),
      ),
      Effect.orElseSucceed(() => DEFAULT_COPILOT_MODELS),
    );

    modelCatalogCache = models;
    return models;
  });

  const ensureTurnState = (context: CopilotSessionContext, turnId: TurnId): CopilotTurnState => {
    const existing = context.turns.find((turn) => turn.id === turnId);
    if (existing) {
      return existing;
    }
    const created: CopilotTurnState = { id: turnId, items: [] };
    context.turns.push(created);
    return created;
  };

  const appendTurnItem = (
    context: CopilotSessionContext,
    turnId: TurnId | undefined,
    item: unknown,
  ): void => {
    if (!turnId) {
      return;
    }
    const turnState = ensureTurnState(context, turnId);
    turnState.items.push(item);
  };

  const mapEventToRuntimeEvents = (
    context: CopilotSessionContext,
    event: CopilotSessionEvent,
  ): Array<ProviderRuntimeEvent> => {
    const createdAt = asString(event.timestamp) ?? nowIso();
    const providerEventId = asString(event.id)?.trim();
    const eventId = EventId.makeUnsafe(providerEventId ?? crypto.randomUUID());
    const data = asObject(event.data);

    const turnIdFromData = toTurnId(data?.turnId);
    const messageId = asString(data?.messageId)?.trim();
    const turnId =
      turnIdFromData ?? (messageId ? context.turnByMessageId.get(messageId) : undefined);

    const base = {
      eventId,
      provider: PROVIDER,
      threadId: context.session.threadId,
      createdAt,
      ...(providerEventId ? { providerEventId } : {}),
    };

    if (event.type === "session.created") {
      context.session = {
        ...context.session,
        status: "ready",
        activeTurnId: undefined,
        resumeCursor: {
          sessionId: context.sdkSession.sessionId,
        },
        updatedAt: createdAt,
      };
      const providerThreadId = asString(data?.sessionId)?.trim();
      return [
        {
          ...base,
          type: "session.started",
          payload: data ? { resume: data } : {},
        },
        {
          ...base,
          type: "thread.started",
          payload: providerThreadId ? { providerThreadId } : {},
        },
      ];
    }

    if (event.type === "session.idle") {
      context.session = {
        ...context.session,
        status: "ready",
        activeTurnId: undefined,
        updatedAt: createdAt,
      };

      const events: ProviderRuntimeEvent[] = [
        {
          ...base,
          type: "session.state.changed",
          payload: {
            state: "waiting",
          },
        },
      ];

      if (turnId) {
        events.push({
          ...base,
          turnId,
          type: "turn.completed",
          payload: {
            state: "completed",
          },
        });
      }

      return events;
    }

    if (event.type === "session.shutdown") {
      context.session = {
        ...context.session,
        status: "closed",
        activeTurnId: undefined,
        updatedAt: createdAt,
      };
      const shutdownType = asString(data?.shutdownType);
      const errorReason = asString(data?.errorReason);
      return [
        {
          ...base,
          type: "session.exited",
          payload: {
            ...(errorReason ? { reason: errorReason } : {}),
            ...(shutdownType === "error"
              ? { exitKind: "error" as const }
              : { exitKind: "graceful" as const }),
          },
        },
      ];
    }

    if (event.type === "session.error") {
      const message = asString(data?.message) ?? "Copilot session error";
      context.session = {
        ...context.session,
        status: "error",
        lastError: message,
        updatedAt: createdAt,
      };
      return [
        {
          ...base,
          type: "runtime.error",
          payload: {
            message,
            class: "provider_error",
            ...(data ? { detail: data } : {}),
          },
        },
      ];
    }

    if (event.type === "assistant.turn_start") {
      const startedTurnId = toTurnId(data?.turnId) ?? TurnId.makeUnsafe(crypto.randomUUID());
      context.currentTurnId = startedTurnId;
      context.session = {
        ...context.session,
        status: "running",
        activeTurnId: startedTurnId,
        updatedAt: createdAt,
      };
      ensureTurnState(context, startedTurnId);
      return [
        {
          ...base,
          turnId: startedTurnId,
          type: "turn.started",
          payload: asString(context.session.model) ? { model: context.session.model } : {},
        },
      ];
    }

    if (event.type === "assistant.turn_end") {
      const endedTurnId = toTurnId(data?.turnId) ?? turnId;
      if (endedTurnId && context.currentTurnId === endedTurnId) {
        context.currentTurnId = undefined;
      }
      context.session = {
        ...context.session,
        activeTurnId: context.currentTurnId,
        updatedAt: createdAt,
      };
      return endedTurnId
        ? [
            {
              ...base,
              turnId: endedTurnId,
              type: "turn.completed",
              payload: {
                state: "completed",
              },
            },
          ]
        : [];
    }

    if (event.type === "abort") {
      const reason = asString(data?.reason) ?? "Turn aborted";
      return [
        {
          ...base,
          type: "turn.aborted",
          payload: {
            reason,
          },
        },
      ];
    }

    if (event.type === "assistant.message_delta") {
      const messageId = asString(data?.messageId)?.trim();
      const delta = asString(data?.deltaContent) ?? asString(data?.delta);
      if (!messageId || !delta) {
        return [];
      }

      const providerItemId =
        context.assistantMessageStateByMessageId.get(messageId)?.itemId ??
        ProviderItemId.makeUnsafe(messageId);
      let messageState = context.assistantMessageStateByMessageId.get(messageId);
      const emitted: ProviderRuntimeEvent[] = [];
      if (!messageState) {
        messageState = {
          itemId: providerItemId,
          sawDelta: false,
        };
        context.assistantMessageStateByMessageId.set(messageId, messageState);
        emitted.push({
          ...base,
          ...(turnId ? { turnId } : {}),
          itemId: asRuntimeItemId(providerItemId),
          type: "item.started",
          payload: {
            itemType: "assistant_message",
            status: "inProgress",
            title: "Assistant message",
          },
          providerRefs: {
            providerItemId,
          },
        });
      }

      if (turnId && !context.turnByMessageId.has(messageId)) {
        context.turnByMessageId.set(messageId, turnId);
      }

      messageState.sawDelta = true;

      emitted.push({
        ...base,
        ...(turnId ? { turnId } : {}),
        itemId: asRuntimeItemId(providerItemId),
        type: "content.delta",
        payload: {
          streamKind: "assistant_text",
          delta,
        },
        providerRefs: {
          providerItemId,
        },
      });

      appendTurnItem(context, turnId, {
        type: event.type,
        data: { ...data, messageId, deltaContent: delta },
      });

      return emitted;
    }

    if (event.type === "assistant.message") {
      const messageId = asString(data?.messageId)?.trim();
      const content = asString(data?.content) ?? asString(data?.text) ?? "";
      if (!messageId) {
        return [];
      }

      const providerItemId =
        context.assistantMessageStateByMessageId.get(messageId)?.itemId ??
        ProviderItemId.makeUnsafe(messageId);
      const messageState = context.assistantMessageStateByMessageId.get(messageId) ?? {
        itemId: providerItemId,
        sawDelta: false,
      };
      const emitted: ProviderRuntimeEvent[] = [];

      if (!context.assistantMessageStateByMessageId.has(messageId)) {
        context.assistantMessageStateByMessageId.set(messageId, messageState);
        emitted.push({
          ...base,
          ...(turnId ? { turnId } : {}),
          itemId: asRuntimeItemId(providerItemId),
          type: "item.started",
          payload: {
            itemType: "assistant_message",
            status: "inProgress",
            title: "Assistant message",
          },
          providerRefs: {
            providerItemId,
          },
        });
      }

      if (turnId && !context.turnByMessageId.has(messageId)) {
        context.turnByMessageId.set(messageId, turnId);
      }

      if (!messageState.sawDelta && content.length > 0) {
        emitted.push({
          ...base,
          ...(turnId ? { turnId } : {}),
          itemId: asRuntimeItemId(providerItemId),
          type: "content.delta",
          payload: {
            streamKind: "assistant_text",
            delta: content,
          },
          providerRefs: {
            providerItemId,
          },
        });
      }

      emitted.push({
        ...base,
        ...(turnId ? { turnId } : {}),
        itemId: asRuntimeItemId(providerItemId),
        type: "item.completed",
        payload: {
          itemType: "assistant_message",
          status: "completed",
          title: "Assistant message",
          ...(content.trim().length > 0 ? { detail: content } : {}),
          ...(data ? { data } : {}),
        },
        providerRefs: {
          providerItemId,
        },
      });

      appendTurnItem(context, turnId, {
        type: event.type,
        data,
      });

      return emitted;
    }

    if (event.type === "assistant.reasoning_delta") {
      const delta = asString(data?.deltaContent);
      if (!delta) {
        return [];
      }
      return [
        {
          ...base,
          type: "content.delta",
          payload: {
            streamKind: "reasoning_text",
            delta,
          },
        },
      ];
    }

    if (event.type === "assistant.reasoning") {
      const reasoningId = asString(data?.reasoningId) ?? crypto.randomUUID();
      const content = asString(data?.content)?.trim();
      const providerItemId = ProviderItemId.makeUnsafe(reasoningId);
      const events: ProviderRuntimeEvent[] = [
        {
          ...base,
          itemId: asRuntimeItemId(providerItemId),
          type: "item.started",
          payload: {
            itemType: "reasoning",
            status: "inProgress",
            title: "Reasoning",
          },
          providerRefs: {
            providerItemId,
          },
        },
      ];

      if (content) {
        events.push({
          ...base,
          itemId: asRuntimeItemId(providerItemId),
          type: "content.delta",
          payload: {
            streamKind: "reasoning_text",
            delta: content,
          },
          providerRefs: {
            providerItemId,
          },
        });
      }

      events.push({
        ...base,
        itemId: asRuntimeItemId(providerItemId),
        type: "item.completed",
        payload: {
          itemType: "reasoning",
          status: "completed",
          title: "Reasoning",
          ...(content ? { detail: content } : {}),
        },
        providerRefs: {
          providerItemId,
        },
      });

      return events;
    }

    if (event.type === "permission.requested") {
      const requestId = toApprovalRequestId(data?.requestId);
      if (!requestId) {
        return [];
      }

      const permissionRequest = asObject(data?.permissionRequest) ?? {};
      const requestType = toCanonicalRequestType(asString(permissionRequest.kind));
      const detail = summarizePermissionRequest(permissionRequest);
      const toolCallId = asString(permissionRequest.toolCallId)?.trim();

      if (!context.pendingPermissions.has(requestId)) {
        const pending = createPendingPermission({
          requestId,
          requestType,
          ...(detail ? { detail } : {}),
          ...(toolCallId ? { toolCallId } : {}),
        });
        context.pendingPermissions.set(requestId, pending);
        context.pendingPermissionOrder.push(requestId);
        if (toolCallId) {
          context.permissionRequestIdByToolCallId.set(toolCallId, requestId);
        }
      }

      return [
        {
          ...base,
          requestId: asRuntimeRequestId(requestId),
          type: "request.opened",
          payload: {
            requestType,
            ...(detail ? { detail } : {}),
            args: data ?? {},
          },
          providerRefs: {
            providerRequestId: requestId,
          },
        },
      ];
    }

    if (event.type === "permission.completed") {
      const requestId = toApprovalRequestId(data?.requestId);
      if (!requestId) {
        return [];
      }

      const result = asObject(data?.result);
      const resultKind = asString(result?.kind);
      const pending = context.pendingPermissions.get(requestId);
      const requestType = pending?.requestType ?? "unknown";
      const decision = permissionResultKindToDecision(resultKind);

      context.pendingPermissions.delete(requestId);
      removeFromOrder(context.pendingPermissionOrder, requestId);
      if (pending?.toolCallId) {
        context.permissionRequestIdByToolCallId.delete(pending.toolCallId);
      }

      return [
        {
          ...base,
          requestId: asRuntimeRequestId(requestId),
          type: "request.resolved",
          payload: {
            requestType,
            ...(decision ? { decision } : {}),
            resolution: data ?? {},
          },
          providerRefs: {
            providerRequestId: requestId,
          },
        },
      ];
    }

    if (event.type === "user_input.requested") {
      const requestId = toApprovalRequestId(data?.requestId);
      const question = asString(data?.question)?.trim();
      if (!requestId || !question) {
        return [];
      }

      const choices = asStringArray(data?.choices);
      const allowFreeform = asBoolean(data?.allowFreeform) ?? true;
      const toolCallId = asString(data?.toolCallId)?.trim();

      if (!context.pendingUserInputs.has(requestId)) {
        const pending = createPendingUserInput({
          requestId,
          question,
          choices,
          allowFreeform,
          ...(toolCallId ? { toolCallId } : {}),
        });
        context.pendingUserInputs.set(requestId, pending);
        context.pendingUserInputOrder.push(requestId);
        if (toolCallId) {
          context.userInputRequestIdByToolCallId.set(toolCallId, requestId);
        }
      }

      return [
        {
          ...base,
          requestId: asRuntimeRequestId(requestId),
          type: "user-input.requested",
          payload: {
            questions: [
              {
                id: "answer",
                header: "User input requested",
                question,
                options: toUserInputOptions(choices),
                multiSelect: false,
              },
            ],
          },
          providerRefs: {
            providerRequestId: requestId,
          },
        },
      ];
    }

    if (event.type === "user_input.completed") {
      const requestId = toApprovalRequestId(data?.requestId);
      if (!requestId) {
        return [];
      }

      const pending = context.pendingUserInputs.get(requestId);
      context.pendingUserInputs.delete(requestId);
      removeFromOrder(context.pendingUserInputOrder, requestId);
      if (pending?.toolCallId) {
        context.userInputRequestIdByToolCallId.delete(pending.toolCallId);
      }

      const answer = asString(data?.answer);
      return [
        {
          ...base,
          requestId: asRuntimeRequestId(requestId),
          type: "user-input.resolved",
          payload: {
            answers: answer ? { answer } : {},
          },
          providerRefs: {
            providerRequestId: requestId,
          },
        },
      ];
    }

    if (event.type === "tool.execution_start") {
      const toolCallId = asString(data?.toolCallId)?.trim() ?? crypto.randomUUID();
      const toolName = asString(data?.toolName);
      const itemType = normalizeToolItemType(toolName);
      const providerItemId = toProviderItemId(toolCallId) ?? ProviderItemId.makeUnsafe(toolCallId);

      return [
        {
          ...base,
          itemId: asRuntimeItemId(providerItemId),
          type: "item.started",
          payload: {
            itemType,
            status: "inProgress",
            ...(itemTitle(itemType) ? { title: itemTitle(itemType) } : {}),
            ...(toolName ? { detail: toolName } : {}),
            ...(data ? { data } : {}),
          },
          providerRefs: {
            providerItemId,
          },
        },
      ];
    }

    if (event.type === "tool.execution_complete") {
      const toolCallId = asString(data?.toolCallId)?.trim() ?? crypto.randomUUID();
      const toolName = asString(data?.toolName);
      const itemType = normalizeToolItemType(toolName);
      const providerItemId = toProviderItemId(toolCallId) ?? ProviderItemId.makeUnsafe(toolCallId);
      const success = asBoolean(data?.success) ?? false;
      const result = asObject(data?.result);
      const error = asObject(data?.error);
      const detail =
        asString(result?.content) ??
        asString(error?.message) ??
        asString(data?.summary) ??
        asString(toolName);

      return [
        {
          ...base,
          itemId: asRuntimeItemId(providerItemId),
          type: "item.completed",
          payload: {
            itemType,
            status: success ? "completed" : "failed",
            ...(itemTitle(itemType) ? { title: itemTitle(itemType) } : {}),
            ...(detail ? { detail } : {}),
            ...(data ? { data } : {}),
          },
          providerRefs: {
            providerItemId,
          },
        },
      ];
    }

    if (event.type === "session.usage_info") {
      const usage = normalizeUsageFromSessionUsageInfo(data ?? {});
      if (!usage) {
        return [];
      }
      return [
        {
          ...base,
          type: "thread.token-usage.updated",
          payload: {
            usage,
          },
        },
      ];
    }

    return [];
  };

  const closeContext = Effect.fn("closeContext")(function* (context: CopilotSessionContext) {
    context.stopped = true;

    if (context.unsubscribe) {
      yield* Effect.try({
        try: () => {
          context.unsubscribe?.();
        },
        catch: () => undefined,
      }).pipe(Effect.orElseSucceed(() => undefined));
      context.unsubscribe = undefined;
    }

    for (const pending of context.pendingPermissions.values()) {
      pending.resolveDecision("cancel");
    }
    for (const pending of context.pendingUserInputs.values()) {
      pending.resolveAnswers({});
    }

    context.pendingPermissions.clear();
    context.permissionRequestIdByToolCallId.clear();
    context.pendingPermissionOrder.length = 0;
    context.pendingUserInputs.clear();
    context.userInputRequestIdByToolCallId.clear();
    context.pendingUserInputOrder.length = 0;

    yield* Effect.tryPromise({
      try: () => context.sdkSession.disconnect(),
      catch: () => undefined,
    }).pipe(Effect.orElseSucceed(() => undefined));
  });

  const readContextOrSessionNotFound = Effect.fn("readContextOrSessionNotFound")(function* (
    threadId: ThreadId,
    operation: string,
  ) {
    const context = sessions.get(threadId);
    if (!context) {
      return yield* new ProviderAdapterSessionNotFoundError({
        provider: PROVIDER,
        threadId,
      });
    }
    if (context.stopped) {
      return yield* new ProviderAdapterSessionClosedError({
        provider: PROVIDER,
        threadId,
      });
    }
    if (context.session.provider !== PROVIDER) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation,
        issue: "Session provider mismatch.",
      });
    }
    return context;
  });

  const startSession: CopilotAdapterShape["startSession"] = Effect.fn("startSession")(
    function* (input) {
      if (input.provider !== undefined && input.provider !== PROVIDER) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
        });
      }

      const existing = sessions.get(input.threadId);
      if (existing && !existing.stopped) {
        return existing.session;
      }

      const client = yield* ensureClient(input.threadId);
      const modelSelection =
        input.modelSelection?.provider === PROVIDER ? input.modelSelection : undefined;
      const requestedModel = modelSelection?.model?.trim();
      const availableModels = yield* listModelsWithFallback(input.threadId);
      const model =
        requestedModel && requestedModel.length > 0
          ? requestedModel
          : (availableModels[0] ?? DEFAULT_COPILOT_MODELS[0]);
      const reasoningEffort = asString(modelSelection?.options?.reasoningEffort)?.trim();

      const pendingPermissions = new Map<ApprovalRequestId, PendingPermission>();
      const permissionRequestIdByToolCallId = new Map<string, ApprovalRequestId>();
      const pendingPermissionOrder: Array<ApprovalRequestId> = [];
      const pendingUserInputs = new Map<ApprovalRequestId, PendingUserInput>();
      const userInputRequestIdByToolCallId = new Map<string, ApprovalRequestId>();
      const pendingUserInputOrder: Array<ApprovalRequestId> = [];

      const onPermissionRequest = async (
        request: unknown,
      ): Promise<{ kind: PermissionResultKind }> => {
        const requestRecord = asObject(request) ?? {};
        const context = sessions.get(input.threadId);
        if (!context || context.stopped) {
          return {
            kind: "denied-no-approval-rule-and-could-not-request-from-user",
          };
        }

        const pending = selectPendingPermission(context, requestRecord);
        if (!pending) {
          return {
            kind: "denied-no-approval-rule-and-could-not-request-from-user",
          };
        }

        const decision = await pending.decisionPromise;
        return {
          kind: decisionToPermissionResultKind(decision),
        };
      };

      const onUserInputRequest = async (
        request: unknown,
      ): Promise<{ answer: string; wasFreeform: boolean }> => {
        const requestRecord = asObject(request) ?? {};
        const context = sessions.get(input.threadId);
        if (!context || context.stopped) {
          return {
            answer: "",
            wasFreeform: true,
          };
        }

        const pending = selectPendingUserInput(context, requestRecord);
        if (!pending) {
          return {
            answer: "",
            wasFreeform: true,
          };
        }

        const answers = await pending.answersPromise;
        const answer = answerFromUserInputAnswers(answers) ?? "";
        return {
          answer,
          wasFreeform: !pending.choices.includes(answer),
        };
      };

      const resumeCursor = asObject(input.resumeCursor);
      const resumeSessionId = asString(resumeCursor?.sessionId)?.trim();
      const sessionConfig: Record<string, unknown> = {
        onPermissionRequest,
        onUserInputRequest,
        streaming: true,
        ...(model ? { model } : {}),
        ...(reasoningEffort ? { reasoningEffort } : {}),
        ...(input.cwd ? { workingDirectory: input.cwd } : {}),
      };

      const sdkSession = yield* Effect.tryPromise({
        try: () =>
          resumeSessionId && resumeSessionId.length > 0
            ? client.resumeSession(resumeSessionId, sessionConfig)
            : client.createSession({
                ...sessionConfig,
                sessionId: input.threadId,
              }),
        catch: (cause) =>
          new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: input.threadId,
            detail: toMessage(cause, "Failed to start Copilot session."),
            cause,
          }),
      });

      const createdAt = nowIso();
      const providerSession: ProviderSession = {
        provider: PROVIDER,
        status: "ready",
        runtimeMode: input.runtimeMode,
        threadId: input.threadId,
        ...(input.cwd ? { cwd: input.cwd } : {}),
        ...(model ? { model } : {}),
        resumeCursor: {
          sessionId: sdkSession.sessionId,
        },
        createdAt,
        updatedAt: createdAt,
      };

      const context: CopilotSessionContext = {
        session: providerSession,
        sdkSession,
        pendingPermissions,
        permissionRequestIdByToolCallId,
        pendingPermissionOrder,
        pendingUserInputs,
        userInputRequestIdByToolCallId,
        pendingUserInputOrder,
        turns: [],
        turnByMessageId: new Map(),
        assistantMessageStateByMessageId: new Map(),
        currentTurnId: undefined,
        stopped: false,
        unsubscribe: undefined,
      };

      sessions.set(input.threadId, context);

      const services = yield* Effect.services<never>();
      const listener = (sdkEvent: CopilotSessionEvent) =>
        Effect.gen(function* () {
          const eventData = asObject(sdkEvent.data);
          const normalizedEvent: CopilotSessionEvent = {
            ...sdkEvent,
            type: asString(sdkEvent.type) ?? "unknown",
            ...(eventData ? { data: eventData } : {}),
          };
          yield* logNativeEvent(input.threadId, normalizedEvent);
          const runtimeEvents = mapEventToRuntimeEvents(context, normalizedEvent);
          if (runtimeEvents.length === 0) {
            return;
          }
          yield* Queue.offerAll(runtimeEventQueue, runtimeEvents);
        }).pipe(Effect.runPromiseWith(services));

      const maybeUnsubscribe = sdkSession.on((event) => {
        void listener(event).catch(() => undefined);
      });

      context.unsubscribe = typeof maybeUnsubscribe === "function" ? maybeUnsubscribe : undefined;

      return context.session;
    },
  );

  const sendTurn: CopilotAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
    const context = yield* readContextOrSessionNotFound(input.threadId, "sendTurn");

    const modelSelection =
      input.modelSelection?.provider === PROVIDER ? input.modelSelection : undefined;
    const selectedModel = modelSelection?.model?.trim();
    const selectedEffort = asString(modelSelection?.options?.reasoningEffort)?.trim();

    if (selectedModel && selectedModel !== context.session.model && context.sdkSession.setModel) {
      yield* Effect.tryPromise({
        try: () =>
          context.sdkSession.setModel!(
            selectedModel,
            selectedEffort ? { reasoningEffort: selectedEffort } : undefined,
          ),
        catch: (cause) => toRequestError(input.threadId, "session.setModel", cause),
      });
    }

    const prompt = input.input?.trim() || DEFAULT_ATTACHMENT_PROMPT;
    const attachments = input.attachments ?? [];
    const sdkAttachments: Array<{ type: string; path: string; displayName?: string }> = [];

    for (const attachment of attachments) {
      if (attachment.type !== "image") {
        continue;
      }
      const attachmentPath = resolveAttachmentPath({
        attachmentsDir: serverConfig.attachmentsDir,
        attachment,
      });
      if (!attachmentPath) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `Invalid attachment id '${attachment.id}'.`,
        });
      }

      // Verify file is readable before sending to the SDK.
      yield* fileSystem.readFile(attachmentPath).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session.send",
              detail: `Failed to read attachment '${attachment.name}'.`,
              cause,
            }),
        ),
      );

      sdkAttachments.push({
        type: "file",
        path: attachmentPath,
        displayName: attachment.name,
      });
    }

    const turnId = TurnId.makeUnsafe(crypto.randomUUID());
    context.currentTurnId = turnId;
    ensureTurnState(context, turnId);

    const messageId = yield* Effect.tryPromise({
      try: () =>
        context.sdkSession.send({
          prompt,
          ...(sdkAttachments.length > 0 ? { attachments: sdkAttachments } : {}),
        }),
      catch: (cause) => toRequestError(input.threadId, "session.send", cause),
    });

    const normalizedMessageId = typeof messageId === "string" ? messageId.trim() : "";
    if (normalizedMessageId.length > 0) {
      context.turnByMessageId.set(normalizedMessageId, turnId);
    }

    context.session = {
      ...context.session,
      status: "running",
      activeTurnId: turnId,
      ...(selectedModel ? { model: selectedModel } : {}),
      resumeCursor: {
        sessionId: context.sdkSession.sessionId,
      },
      updatedAt: nowIso(),
    };

    return {
      threadId: input.threadId,
      turnId,
      resumeCursor: {
        sessionId: context.sdkSession.sessionId,
      },
    };
  });

  const interruptTurn: CopilotAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
    function* (threadId, _turnId) {
      const context = yield* readContextOrSessionNotFound(threadId, "interruptTurn");
      yield* Effect.tryPromise({
        try: () => context.sdkSession.abort(),
        catch: (cause) => toRequestError(threadId, "session.abort", cause),
      });
    },
  );

  const respondToRequest: CopilotAdapterShape["respondToRequest"] = Effect.fn("respondToRequest")(
    function* (threadId, requestId, decision) {
      const context = yield* readContextOrSessionNotFound(threadId, "respondToRequest");
      const pending = context.pendingPermissions.get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToRequest",
          detail: `Unknown pending approval request '${requestId}'.`,
        });
      }
      pending.resolveDecision(decision);
    },
  );

  const respondToUserInput: CopilotAdapterShape["respondToUserInput"] = Effect.fn(
    "respondToUserInput",
  )(function* (threadId, requestId, answers) {
    const context = yield* readContextOrSessionNotFound(threadId, "respondToUserInput");
    const pending = context.pendingUserInputs.get(requestId);
    if (!pending) {
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToUserInput",
        detail: `Unknown pending user input request '${requestId}'.`,
      });
    }
    pending.resolveAnswers(answers);
  });

  const stopSession: CopilotAdapterShape["stopSession"] = (threadId) =>
    Effect.gen(function* () {
      const context = sessions.get(threadId);
      if (!context) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId,
        });
      }

      yield* closeContext(context);
      sessions.delete(threadId);
    });

  const listSessions: CopilotAdapterShape["listSessions"] = () =>
    Effect.sync(() =>
      Array.from(sessions.values())
        .filter((context) => !context.stopped)
        .map((context) => context.session),
    );

  const hasSession: CopilotAdapterShape["hasSession"] = (threadId) =>
    Effect.sync(() => {
      const context = sessions.get(threadId);
      return context !== undefined && !context.stopped;
    });

  const readThread: CopilotAdapterShape["readThread"] = Effect.fn("readThread")(
    function* (threadId) {
      const context = yield* readContextOrSessionNotFound(threadId, "readThread");
      return {
        threadId,
        turns: context.turns.map((turn) => ({
          id: turn.id,
          items: [...turn.items],
        })),
      } satisfies ProviderThreadSnapshot;
    },
  );

  const rollbackThread: CopilotAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
    function* (threadId, numTurns) {
      const _context = yield* readContextOrSessionNotFound(threadId, "rollbackThread");
      if (numTurns === 0) {
        return yield* readThread(threadId);
      }

      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "rollbackThread",
        detail:
          "Copilot session rollback is not supported by this adapter yet. Start a new session to reset context.",
      });
    },
  );

  const stopAll: CopilotAdapterShape["stopAll"] = () =>
    Effect.gen(function* () {
      const activeContexts = Array.from(sessions.values());
      for (const context of activeContexts) {
        yield* closeContext(context);
      }
      sessions.clear();
      yield* stopClient();
    });

  yield* Effect.addFinalizer(() =>
    stopAll().pipe(Effect.ignore({ log: true }), Effect.andThen(Queue.shutdown(runtimeEventQueue))),
  );

  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "in-session",
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    streamEvents: Stream.fromQueue(runtimeEventQueue),
  } satisfies CopilotAdapterShape;
});

export const CopilotAdapterLive = Layer.effect(CopilotAdapter, makeCopilotAdapter());

export function makeCopilotAdapterLive(options?: CopilotAdapterLiveOptions) {
  return Layer.effect(CopilotAdapter, makeCopilotAdapter(options));
}
