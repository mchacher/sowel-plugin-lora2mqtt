import { EventEmitter } from "node:events";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// A fake MQTT client: records publishes, lets a test inject messages.
class FakeClient extends EventEmitter {
  connected = true;
  published: { topic: string; payload: string }[] = [];
  subscribe(_topic: string, cb?: (err: Error | null) => void) { cb?.(null); }
  publish(topic: string, payload: string, cb?: (err?: Error) => void) {
    this.published.push({ topic, payload }); cb?.();
  }
  async endAsync() { this.connected = false; }
}

let client: FakeClient;
vi.mock("mqtt", () => ({
  default: {
    connect: () => {
      client = new FakeClient();
      setImmediate(() => client.emit("connect"));
      return client;
    },
  },
}));

const { createPlugin } = await import("./index.js");
const { RECEIPT_FALLBACK_MS, RECEIPT_RETENTION_MS } = await import("./receipts.js");

const noop = () => {};
const logger = { child: () => logger, info: noop, warn: noop, error: noop, debug: vi.fn() };

const GATE = { id: "dev-gate", integrationId: "lora2mqtt", sourceDeviceId: "portail", name: "Portail" };
const COOP = { id: "dev-coop", integrationId: "lora2mqtt", sourceDeviceId: "poulailler", name: "Poulailler" };

function receiptEvents(emit: ReturnType<typeof vi.fn>) {
  return emit.mock.calls.map((c) => c[0]).filter((e) => e.type === "device.order.receipt");
}

function inject(topic: string, payload: unknown) {
  client.emit("message", topic, Buffer.from(JSON.stringify(payload)));
}

async function startPlugin() {
  const emit = vi.fn();
  const settings: Record<string, string> = { "integration.lora2mqtt.mqtt_url": "mqtt://broker" };
  const plugin = createPlugin({
    logger: logger as never,
    eventBus: { emit },
    settingsManager: { get: (k: string) => settings[k], set: noop },
    deviceManager: {
      upsertFromDiscovery: noop, updateDeviceData: noop, updateDeviceStatus: noop,
      removeStaleDevices: noop, logSummary: noop,
    },
    pluginDir: "/tmp",
  });
  await plugin.start();
  inject("lora2mqtt/bridge/devices", [
    { node_id: 3, friendly_name: "portail", is_active: true, receipts: true,
      data_keys: { gate: { type: "numeric", access: "rw", category: "gate_state" } } },
    { node_id: 4, friendly_name: "poulailler", is_active: true,
      data_keys: { door: { type: "numeric", access: "rw" } } },
  ]);
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  return { plugin, emit };
}

async function sendGateOrder(plugin: Awaited<ReturnType<typeof startPlugin>>["plugin"]) {
  await plugin.executeOrder(GATE, "gate", 1);
  const last = client.published.at(-1)!;
  return JSON.parse(last.payload)["#id"] as string;
}

describe("order receipts (spec 188 R6)", () => {
  beforeEach(() => { logger.debug.mockClear(); });
  afterEach(() => { vi.useRealTimers(); });

  it("tags an order on a receipts node, emits pending and returns at once", async () => {
    const { plugin, emit } = await startPlugin();
    await plugin.executeOrder(GATE, "gate", 1);

    const last = client.published.at(-1)!;
    expect(last.topic).toBe("lora2mqtt/portail/set");
    const body = JSON.parse(last.payload);
    expect(body.gate).toBe(1);
    expect(body["#id"]).toMatch(/^[0-9a-f-]{36}$/);
    expect(receiptEvents(emit)).toEqual([
      { type: "device.order.receipt", integrationId: "lora2mqtt", deviceId: "dev-gate", orderKey: "gate", status: "pending" },
    ]);
    await plugin.stop();
  });

  it("leaves a node without receipts exactly as before", async () => {
    const { plugin, emit } = await startPlugin();
    await plugin.executeOrder(COOP, "door", 1);

    expect(client.published.at(-1)).toEqual({ topic: "lora2mqtt/poulailler/set", payload: '{"door":1}' });
    vi.advanceTimersByTime(RECEIPT_FALLBACK_MS * 2);
    expect(receiptEvents(emit)).toEqual([]);
    await plugin.stop();
  });

  it.each([
    ["received", "received"],
    ["applied", "delivered"],
    ["refused", "refused"],
    ["not_received", "not_received"],
    ["timeout", "unconfirmed"],
  ])("maps the bridge result %s to %s", async (result, status) => {
    const { plugin, emit } = await startPlugin();
    const id = await sendGateOrder(plugin);
    inject("lora2mqtt/portail/receipt", { id, counter: 12, result, ms: 245, late: false });

    expect(receiptEvents(emit).at(-1)).toEqual({
      type: "device.order.receipt", integrationId: "lora2mqtt", deviceId: "dev-gate", orderKey: "gate", status,
    });
    await plugin.stop();
  });

  it("passes the bridge's late flag through", async () => {
    const { plugin, emit } = await startPlugin();
    const id = await sendGateOrder(plugin);
    inject("lora2mqtt/portail/receipt", { id, counter: 12, result: "timeout", ms: 15000, late: false });
    inject("lora2mqtt/portail/receipt", { id, counter: 12, result: "applied", ms: 17000, late: true });

    expect(receiptEvents(emit).map((e) => [e.status, e.late])).toEqual([
      ["pending", undefined], ["unconfirmed", undefined], ["delivered", true],
    ]);
    await plugin.stop();
  });

  it("marks an applied receipt after not_received as late", async () => {
    const { plugin, emit } = await startPlugin();
    const id = await sendGateOrder(plugin);
    inject("lora2mqtt/portail/receipt", { id, counter: 12, result: "not_received", ms: 1100, late: false });
    inject("lora2mqtt/portail/receipt", { id, counter: 12, result: "applied", ms: 2400, late: false });

    expect(receiptEvents(emit).at(-1)).toMatchObject({ status: "delivered", late: true });
    await plugin.stop();
  });

  it("says unconfirmed itself after 20 s of silence, and still maps a later receipt", async () => {
    const { plugin, emit } = await startPlugin();
    const id = await sendGateOrder(plugin);
    inject("lora2mqtt/portail/receipt", { id, counter: 12, result: "received", ms: 300, late: false });

    vi.advanceTimersByTime(RECEIPT_FALLBACK_MS - 1);
    expect(receiptEvents(emit).map((e) => e.status)).toEqual(["pending", "received"]);
    vi.advanceTimersByTime(1);
    expect(receiptEvents(emit).at(-1)).toEqual({
      type: "device.order.receipt", integrationId: "lora2mqtt", deviceId: "dev-gate", orderKey: "gate", status: "unconfirmed",
    });

    inject("lora2mqtt/portail/receipt", { id, counter: 12, result: "applied", ms: 25000, late: true });
    expect(receiptEvents(emit).at(-1)).toMatchObject({ status: "delivered", late: true });
    await plugin.stop();
  });

  it("does not repeat unconfirmed when the bridge's timeout follows its own fallback", async () => {
    const { plugin, emit } = await startPlugin();
    const id = await sendGateOrder(plugin);
    vi.advanceTimersByTime(RECEIPT_FALLBACK_MS);
    inject("lora2mqtt/portail/receipt", { id, counter: 12, result: "timeout", ms: 15000, late: false });

    expect(receiptEvents(emit).map((e) => e.status)).toEqual(["pending", "unconfirmed"]);
    await plugin.stop();
  });

  it("does not fall back once a terminal receipt came", async () => {
    const { plugin, emit } = await startPlugin();
    const id = await sendGateOrder(plugin);
    inject("lora2mqtt/portail/receipt", { id, counter: 12, result: "applied", ms: 245, late: false });
    vi.advanceTimersByTime(RECEIPT_FALLBACK_MS * 2);

    expect(receiptEvents(emit).map((e) => e.status)).toEqual(["pending", "delivered"]);
    await plugin.stop();
  });

  it("forgets a terminal order after the retention window", async () => {
    const { plugin, emit } = await startPlugin();
    const id = await sendGateOrder(plugin);
    inject("lora2mqtt/portail/receipt", { id, counter: 12, result: "timeout", ms: 15000, late: false });
    vi.advanceTimersByTime(RECEIPT_RETENTION_MS);
    inject("lora2mqtt/portail/receipt", { id, counter: 12, result: "applied", ms: 140000, late: true });

    expect(receiptEvents(emit).map((e) => e.status)).toEqual(["pending", "unconfirmed"]);
    await plugin.stop();
  });

  it("ignores unknown and absent ids", async () => {
    const { plugin, emit } = await startPlugin();
    await sendGateOrder(plugin);
    inject("lora2mqtt/portail/receipt", { id: "someone-else", counter: 9, result: "applied", ms: 200, late: false });
    inject("lora2mqtt/portail/receipt", { id: null, counter: 10, result: "applied", ms: 200, late: false });
    client.emit("message", "lora2mqtt/portail/receipt", Buffer.from("not json"));

    expect(receiptEvents(emit).map((e) => e.status)).toEqual(["pending"]);
    expect(logger.debug).toHaveBeenCalledTimes(2);
    await plugin.stop();
  });

  it("cancels every timer on stop", async () => {
    const { plugin, emit } = await startPlugin();
    await sendGateOrder(plugin);
    await plugin.stop();
    vi.advanceTimersByTime(RECEIPT_FALLBACK_MS * 2);

    expect(receiptEvents(emit).map((e) => e.status)).toEqual(["pending"]);
    expect(vi.getTimerCount()).toBe(0);
  });
});
