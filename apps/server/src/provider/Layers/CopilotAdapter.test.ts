import assert from "node:assert/strict";
import {
  ApprovalRequestId,
  ThreadId,
  type ProviderRuntimeEvent,
  type ProviderSessionStartInput,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, it } from "@effect/vitest";
import { vi } from "vitest";
import { TestClock } from "effect/testing";

vi.mock("effect", async () => {
  const actual = await vi.importActual<typeof import("effect")>("effect");
  const effectCompat = new Proxy(actual.Effect, {
    get(target, property, receiver) {
      if (property === "catchAll") {
        return target.catch;
      }
      if (property === "ignoreLogged") {
        return (self: unknown) =>
          (self as { pipe: (...ops: ReadonlyArray<unknown>) => unknown }).pipe(
            target.ignore({ log: true }),
          );
      }
      return Reflect.get(target, property, receiver);
    },
  });

  return {
    ...actual,
    Effect: effectCompat,
  };
});

import { Effect, Fiber, Layer, Option, Stream } from "effect";

import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { CopilotAdapter } from "../Services/CopilotAdapter.ts";
import { makeCopilotAdapterLive } from "./CopilotAdapter.ts";

const asThreadId = (value: string): ThreadId => ThreadId.makeUnsafe(value);
const asRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.makeUnsafe(value);

const VALID_START_INPUT: ProviderSessionStartInput = {
  provider: "copilot",
  threadId: asThreadId("thread-copilot-1"),
  runtimeMode: "full-access",
};

const EVENT_TIME = "2026-04-05T00:00:00.000Z";

type PermissionResultKind =
  | "approved"
  | "denied-by-rules"
  | "denied-no-approval-rule-and-could-not-request-from-user"
  | "denied-interactively-by-user"
  | "denied-by-content-exclusion-policy"
  | "denied-by-permission-request-hook";

interface CopilotSessionEvent {
  readonly type: string;
  readonly id?: string;
  readonly timestamp?: string;
  readonly data?: Record<string, unknown>;
  readonly [key: string]: unknown;
}

interface SessionConfig {
  readonly onPermissionRequest?: (request: unknown) => Promise<{
    kind: PermissionResultKind;
  }>;
  readonly onUserInputRequest?: (request: unknown) => Promise<{
    answer: string;
    wasFreeform: boolean;
  }>;
  readonly streaming?: boolean;
  readonly model?: string;
  readonly reasoningEffort?: string;
  readonly workingDirectory?: string;
  readonly sessionId?: string;
}

type SendInput = {
  readonly prompt: string;
  readonly attachments?: ReadonlyArray<{
    readonly type: string;
    readonly path?: string;
    readonly displayName?: string;
  }>;
  readonly mode?: "enqueue" | "immediate";
};

type SetModelOptions = {
  reasoningEffort?: string;
};

type CreateClient = NonNullable<
  NonNullable<Parameters<typeof makeCopilotAdapterLive>[0]>["createClient"]
>;
type CopilotClientLikeForTest = Awaited<ReturnType<CreateClient>>;

class FakeCopilotSession {
  private readonly listeners = new Set<(event: CopilotSessionEvent) => void>();

  readonly sendImpl = vi.fn(async (_options: SendInput) => "msg-1");
  readonly abortImpl = vi.fn(async () => undefined);
  readonly disconnectImpl = vi.fn(async () => undefined);
  readonly setModelImpl = vi.fn(async (_model: string, _options?: SetModelOptions) => undefined);

  constructor(readonly sessionId: string) {}

  emit(event: CopilotSessionEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  send(options: SendInput): Promise<string> {
    return this.sendImpl(options);
  }

  on(handler: (event: CopilotSessionEvent) => void): () => void {
    this.listeners.add(handler);
    return () => {
      this.listeners.delete(handler);
    };
  }

  abort(): Promise<void> {
    return this.abortImpl();
  }

  disconnect(): Promise<void> {
    return this.disconnectImpl();
  }

  setModel(model: string, options?: SetModelOptions): Promise<void> {
    return this.setModelImpl(model, options);
  }
}

class FakeCopilotClient {
  readonly startImpl = vi.fn(async () => undefined);
  readonly stopImpl = vi.fn(async () => undefined);
  readonly createSessionImpl = vi.fn(async (_config: Record<string, unknown>) => this.session);
  readonly resumeSessionImpl = vi.fn(
    async (_sessionId: string, _config: Record<string, unknown>) => this.session,
  );
  readonly listModelsImpl = vi.fn(async () => [{ id: "gpt-5.4" }]);

  lastCreateSessionConfig: SessionConfig | undefined;

  constructor(readonly session: FakeCopilotSession) {}

  start(): Promise<void> {
    return this.startImpl();
  }

  stop(): Promise<unknown> {
    return this.stopImpl();
  }

  createSession(config: Record<string, unknown>): Promise<FakeCopilotSession> {
    this.lastCreateSessionConfig = config as SessionConfig;
    return this.createSessionImpl(config);
  }

  resumeSession(sessionId: string, config: Record<string, unknown>): Promise<FakeCopilotSession> {
    this.lastCreateSessionConfig = config as SessionConfig;
    return this.resumeSessionImpl(sessionId, config);
  }

  listModels(): Promise<ReadonlyArray<unknown>> {
    return this.listModelsImpl();
  }
}

function makeServerSettingsLayer(binaryPath = "copilot-test-bin") {
  return Layer.succeed(ServerSettingsService, {
    start: Effect.void,
    ready: Effect.void,
    getSettings: Effect.succeed({
      providers: {
        copilot: {
          binaryPath,
        },
      },
    } as never),
    updateSettings: () => Effect.die(new Error("ServerSettingsService.updateSettings not used")),
    streamChanges: Stream.empty,
  });
}

function makeHarness(options?: {
  readonly createClientFailure?: unknown;
  readonly clientStartFailure?: unknown;
}) {
  const session = new FakeCopilotSession("sdk-session-1");
  const client = new FakeCopilotClient(session);
  const clientLike: Partial<CopilotClientLikeForTest> = client;

  if (options?.clientStartFailure !== undefined) {
    client.startImpl.mockImplementationOnce(async () => {
      throw options.clientStartFailure;
    });
  }

  const createClientImpl = vi.fn<CreateClient>(async (_input) => {
    if (options?.createClientFailure !== undefined) {
      throw options.createClientFailure;
    }
    return clientLike as CopilotClientLikeForTest;
  });

  return {
    layer: makeCopilotAdapterLive({
      createClient: createClientImpl,
    }).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(makeServerSettingsLayer()),
      Layer.provideMerge(NodeServices.layer),
    ),
    session,
    client,
    createClientImpl,
  };
}

describe("CopilotAdapterLive session lifecycle", () => {
  it.effect("starts, lists, and stops sessions on happy path", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const started = yield* adapter.startSession(VALID_START_INPUT);

      assert.equal(started.provider, "copilot");
      assert.equal(started.status, "ready");
      assert.equal(started.threadId, VALID_START_INPUT.threadId);
      assert.equal(started.resumeCursor !== undefined, true);

      const sessions = yield* adapter.listSessions();
      assert.equal(sessions.length, 1);
      assert.equal(sessions[0]?.threadId, VALID_START_INPUT.threadId);

      yield* adapter.stopSession(VALID_START_INPUT.threadId);
      const hasSession = yield* adapter.hasSession(VALID_START_INPUT.threadId);
      assert.equal(hasSession, false);
      assert.equal(harness.session.disconnectImpl.mock.calls.length, 1);
    }).pipe(Effect.provide(harness.layer));
  });

  it.effect("fails sendTurn when session does not exist", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const result = yield* adapter
        .sendTurn({
          threadId: asThreadId("thread-missing"),
          input: "hello",
          attachments: [],
        })
        .pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") {
        return;
      }

      assert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
      if (result.failure._tag !== "ProviderAdapterSessionNotFoundError") {
        return;
      }
      assert.equal(result.failure.provider, "copilot");
      assert.equal(result.failure.threadId, "thread-missing");
    }).pipe(Effect.provide(harness.layer));
  });

  it.effect("fails stopSession when session does not exist", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const result = yield* adapter.stopSession(asThreadId("thread-missing")).pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") {
        return;
      }

      assert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
      if (result.failure._tag !== "ProviderAdapterSessionNotFoundError") {
        return;
      }
      assert.equal(result.failure.provider, "copilot");
      assert.equal(result.failure.threadId, "thread-missing");
    }).pipe(Effect.provide(harness.layer));
  });
});

describe("CopilotAdapterLive streaming event mapping", () => {
  it.effect(
    "sends a user prompt and maps assistant deltas/completion into runtime events in order",
    () => {
      const harness = makeHarness();
      return Effect.gen(function* () {
        const adapter = yield* CopilotAdapter;
        yield* adapter.startSession(VALID_START_INPUT);

        const eventsFiber = yield* Stream.take(adapter.streamEvents, 3).pipe(
          Stream.runCollect,
          Effect.forkChild,
        );

        const sendResult = yield* adapter.sendTurn({
          threadId: VALID_START_INPUT.threadId,
          input: "Say hello from Copilot",
          attachments: [],
        });

        assert.equal(harness.session.sendImpl.mock.calls.length, 1);
        assert.deepEqual(harness.session.sendImpl.mock.calls[0]?.[0], {
          prompt: "Say hello from Copilot",
        });

        harness.session.emit({
          type: "assistant.message_delta",
          id: "evt-msg-delta-flow",
          timestamp: EVENT_TIME,
          data: {
            messageId: "msg-1",
            deltaContent: "hello ",
          },
        });

        harness.session.emit({
          type: "assistant.message",
          id: "evt-msg-complete-flow",
          timestamp: EVENT_TIME,
          data: {
            messageId: "msg-1",
            content: "hello world",
          },
        });

        const events = Array.from(yield* Fiber.join(eventsFiber)) as Array<ProviderRuntimeEvent>;
        assert.equal(events.length, 3);

        assert.equal(events[1]?.type, "content.delta");
        if (events[1]?.type === "content.delta") {
          assert.equal(events[1].turnId, sendResult.turnId);
          assert.equal(events[1].payload.streamKind, "assistant_text");
          assert.equal(events[1].payload.delta, "hello ");
        }

        assert.equal(events[2]?.type, "item.completed");
        if (events[2]?.type === "item.completed") {
          assert.equal(events[2].turnId, sendResult.turnId);
          assert.equal(events[2].payload.itemType, "assistant_message");
          assert.equal(events[2].payload.status, "completed");
          assert.equal(events[2].payload.detail, "hello world");
        }
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect(
    "maps assistant.message_delta and assistant.message into canonical runtime events",
    () => {
      const harness = makeHarness();
      return Effect.gen(function* () {
        const adapter = yield* CopilotAdapter;
        yield* adapter.startSession(VALID_START_INPUT);

        const eventsFiber = yield* Stream.take(adapter.streamEvents, 3).pipe(
          Stream.runCollect,
          Effect.forkChild,
        );

        harness.session.emit({
          type: "assistant.message_delta",
          id: "evt-msg-delta",
          timestamp: EVENT_TIME,
          data: {
            turnId: "turn-1",
            messageId: "msg-1",
            deltaContent: "hello ",
          },
        });

        harness.session.emit({
          type: "assistant.message",
          id: "evt-msg-complete",
          timestamp: EVENT_TIME,
          data: {
            turnId: "turn-1",
            messageId: "msg-1",
            content: "hello world",
          },
        });

        const events = Array.from(yield* Fiber.join(eventsFiber)) as Array<ProviderRuntimeEvent>;
        assert.equal(events.length, 3);

        assert.equal(events[0]?.type, "item.started");
        if (events[0]?.type === "item.started") {
          assert.equal(events[0].turnId, "turn-1");
          assert.equal(events[0].payload.itemType, "assistant_message");
          assert.equal(events[0].itemId, "msg-1");
        }

        assert.equal(events[1]?.type, "content.delta");
        if (events[1]?.type === "content.delta") {
          assert.equal(events[1].turnId, "turn-1");
          assert.equal(events[1].payload.streamKind, "assistant_text");
          assert.equal(events[1].payload.delta, "hello ");
        }

        assert.equal(events[2]?.type, "item.completed");
        if (events[2]?.type === "item.completed") {
          assert.equal(events[2].turnId, "turn-1");
          assert.equal(events[2].payload.itemType, "assistant_message");
          assert.equal(events[2].payload.status, "completed");
          assert.equal(events[2].payload.detail, "hello world");
        }
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect("ignores assistant.message_delta events without deltaContent", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      yield* adapter.startSession(VALID_START_INPUT);

      const maybeEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(
        Effect.timeoutOption("100 millis"),
        Effect.forkChild,
      );

      yield* Effect.yieldNow;

      harness.session.emit({
        type: "assistant.message_delta",
        id: "evt-missing-delta",
        timestamp: EVENT_TIME,
        data: {
          turnId: "turn-2",
          messageId: "msg-missing-delta",
        },
      });

      yield* Effect.yieldNow;
      yield* TestClock.adjust("100 millis");

      const maybeEvent = yield* Fiber.join(maybeEventFiber);
      assert.equal(Option.isNone(maybeEvent), true);
    }).pipe(Effect.provide(Layer.merge(harness.layer, TestClock.layer())));
  });

  it.effect("ignores assistant.message events without messageId", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      yield* adapter.startSession(VALID_START_INPUT);

      const maybeEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(
        Effect.timeoutOption("100 millis"),
        Effect.forkChild,
      );

      yield* Effect.yieldNow;

      harness.session.emit({
        type: "assistant.message",
        id: "evt-missing-message-id",
        timestamp: EVENT_TIME,
        data: {
          turnId: "turn-3",
          content: "orphan content",
        },
      });

      yield* Effect.yieldNow;
      yield* TestClock.adjust("100 millis");

      const maybeEvent = yield* Fiber.join(maybeEventFiber);
      assert.equal(Option.isNone(maybeEvent), true);
    }).pipe(Effect.provide(Layer.merge(harness.layer, TestClock.layer())));
  });
});

describe("CopilotAdapterLive permission forwarding", () => {
  it.effect("forwards approval decisions through onPermissionRequest on happy path", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      yield* adapter.startSession(VALID_START_INPUT);

      const openedEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      harness.session.emit({
        type: "permission.requested",
        id: "evt-perm-requested",
        timestamp: EVENT_TIME,
        data: {
          requestId: "req-1",
          permissionRequest: {
            kind: "shell",
            toolCallId: "tool-call-1",
            fullCommandText: "git status",
          },
        },
      });

      const openedEvent = yield* Fiber.join(openedEventFiber);
      assert.equal(openedEvent._tag, "Some");
      if (openedEvent._tag !== "Some") {
        return;
      }
      assert.equal(openedEvent.value.type, "request.opened");

      const onPermissionRequest = harness.client.lastCreateSessionConfig?.onPermissionRequest;
      assert.equal(typeof onPermissionRequest, "function");
      if (!onPermissionRequest) {
        return;
      }

      const handlerPromise = onPermissionRequest({
        requestId: "req-1",
        toolCallId: "tool-call-1",
      });

      yield* adapter.respondToRequest(VALID_START_INPUT.threadId, asRequestId("req-1"), "accept");

      const permissionResult = yield* Effect.promise(() => handlerPromise);
      assert.deepEqual(permissionResult, {
        kind: "approved",
      });
    }).pipe(Effect.provide(harness.layer));
  });

  it.effect("fails respondToRequest when session is missing", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const result = yield* adapter
        .respondToRequest(asThreadId("thread-missing"), asRequestId("req-2"), "accept")
        .pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") {
        return;
      }
      assert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
    }).pipe(Effect.provide(harness.layer));
  });

  it.effect("fails respondToRequest when request id is unknown", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      yield* adapter.startSession(VALID_START_INPUT);

      const result = yield* adapter
        .respondToRequest(VALID_START_INPUT.threadId, asRequestId("req-unknown"), "decline")
        .pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") {
        return;
      }
      assert.equal(result.failure._tag, "ProviderAdapterRequestError");
      if (result.failure._tag !== "ProviderAdapterRequestError") {
        return;
      }
      assert.equal(result.failure.provider, "copilot");
      assert.equal(result.failure.method, "respondToRequest");
    }).pipe(Effect.provide(harness.layer));
  });
});

describe("CopilotAdapterLive health check path", () => {
  it.effect("initializes client and creates a Copilot session on healthy path", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const session = yield* adapter.startSession(VALID_START_INPUT);

      assert.equal(session.provider, "copilot");
      assert.equal(harness.createClientImpl.mock.calls.length, 1);
      assert.deepEqual(harness.createClientImpl.mock.calls[0]?.[0], {
        binaryPath: "copilot-test-bin",
      });
      assert.equal(harness.client.startImpl.mock.calls.length, 1);
      assert.equal(harness.client.createSessionImpl.mock.calls.length, 1);
      const startInvocationOrder = harness.client.startImpl.mock.invocationCallOrder[0];
      const createSessionInvocationOrder =
        harness.client.createSessionImpl.mock.invocationCallOrder[0];
      if (startInvocationOrder === undefined || createSessionInvocationOrder === undefined) {
        assert.fail("Expected start/createSession invocation order entries");
      }
      assert.equal(startInvocationOrder < createSessionInvocationOrder, true);
    }).pipe(Effect.provide(harness.layer));
  });

  it.effect("fails startSession when client creation fails", () => {
    const harness = makeHarness({
      createClientFailure: new Error("copilot sdk unavailable"),
    });
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const result = yield* adapter.startSession(VALID_START_INPUT).pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") {
        return;
      }

      assert.equal(result.failure._tag, "ProviderAdapterProcessError");
      if (result.failure._tag !== "ProviderAdapterProcessError") {
        return;
      }
      assert.equal(result.failure.provider, "copilot");
      assert.equal(harness.client.startImpl.mock.calls.length, 0);
      assert.equal(harness.client.createSessionImpl.mock.calls.length, 0);
    }).pipe(Effect.provide(harness.layer));
  });

  it.effect("fails startSession when client start fails", () => {
    const harness = makeHarness({
      clientStartFailure: new Error("client start crashed"),
    });
    return Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const result = yield* adapter.startSession(VALID_START_INPUT).pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") {
        return;
      }

      assert.equal(result.failure._tag, "ProviderAdapterProcessError");
      if (result.failure._tag !== "ProviderAdapterProcessError") {
        return;
      }
      assert.equal(result.failure.provider, "copilot");
      assert.equal(harness.client.createSessionImpl.mock.calls.length, 0);
    }).pipe(Effect.provide(harness.layer));
  });
});
