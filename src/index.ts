/**
 * Sowel Plugin: LoRa2MQTT
 *
 * Integrates LoRa devices via a lora2mqtt MQTT bridge.
 * Subscribes to bridge/devices for discovery, device state, and availability.
 * Supports read + write (orders via MQTT publish).
 */

import { randomUUID } from "node:crypto";
import mqtt, { type MqttClient, type IClientOptions } from "mqtt";
import { ReceiptTracker, type OrderReceiptEvent } from "./receipts.js";

// ============================================================
// Local type definitions
// ============================================================

interface Logger {
  child(bindings: Record<string, unknown>): Logger;
  info(obj: Record<string, unknown>, msg: string): void;
  info(msg: string): void;
  warn(obj: Record<string, unknown>, msg: string): void;
  warn(msg: string): void;
  error(obj: Record<string, unknown>, msg: string): void;
  error(msg: string): void;
  debug(obj: Record<string, unknown>, msg: string): void;
  debug(msg: string): void;
}

type PluginEvent =
  | { type: "system.integration.connected"; integrationId: string }
  | { type: "system.integration.disconnected"; integrationId: string }
  | OrderReceiptEvent;

interface EventBus { emit(event: PluginEvent): void; }
interface SettingsManager { get(key: string): string | undefined; set(key: string, value: string): void; }

interface DiscoveredDevice {
  ieeeAddress?: string; friendlyName: string; manufacturer?: string; model?: string;
  rawExpose?: unknown;
  data: { key: string; type: string; category: string; unit?: string }[];
  orders: { key: string; type: string; category?: string; dispatchConfig?: Record<string, unknown>; min?: number; max?: number; enumValues?: string[]; unit?: string }[];
}

interface DeviceManager {
  upsertFromDiscovery(integrationId: string, source: string, discovered: DiscoveredDevice): void;
  updateDeviceData(integrationId: string, sourceDeviceId: string, payload: Record<string, unknown>): void;
  updateDeviceStatus(integrationId: string, sourceDeviceId: string, status: string): void;
  removeStaleDevices(integrationId: string, activeIds: Set<string>): void;
  logSummary(): void;
}

interface Device { id: string; integrationId: string; sourceDeviceId: string; name: string; }
interface PluginDeps { logger: Logger; eventBus: EventBus; settingsManager: SettingsManager; deviceManager: DeviceManager; pluginDir: string; }

type IntegrationStatus = "connected" | "disconnected" | "not_configured" | "error";
interface IntegrationSettingDef { key: string; label: string; type: "text" | "password" | "number" | "boolean"; required: boolean; placeholder?: string; defaultValue?: string; }

interface IntegrationPlugin {
  readonly id: string; readonly name: string; readonly description: string; readonly icon: string;
  readonly apiVersion?: number;
  getStatus(): IntegrationStatus; isConfigured(): boolean; getSettingsSchema(): IntegrationSettingDef[];
  start(options?: { pollOffset?: number }): Promise<void>; stop(): Promise<void>;
  executeOrder(device: Device, orderKeyOrDispatchConfig: string | Record<string, unknown>, value: unknown): Promise<void>;
  refresh?(): Promise<void>; getPollingInfo?(): { lastPollAt: string; intervalMs: number } | null;
}

// ============================================================
// Constants (inlined from Sowel shared/constants.ts)
// ============================================================

const INTEGRATION_ID = "lora2mqtt";
const SETTINGS_PREFIX = `integration.${INTEGRATION_ID}.`;

type DataType = "number" | "boolean" | "enum" | "text";
type DataCategory = string;

const LORA_TYPE_TO_DATA_TYPE: Record<string, DataType> = {
  numeric: "number",
  binary: "boolean",
  enum: "enum",
};

const PROPERTY_TO_CATEGORY: Record<string, DataCategory> = {
  occupancy: "motion", presence: "motion",
  temperature: "temperature", device_temperature: "temperature", soil_temperature: "temperature",
  humidity: "humidity", soil_moisture: "humidity",
  pressure: "pressure",
  illuminance: "luminosity", illuminance_lux: "luminosity",
  battery: "battery",
  voltage: "voltage",
  power: "power", energy: "energy",
  co2: "co2",
  action: "action",
  contact: "contact",
  state: "light_state",
  brightness: "light_brightness",
  color_temp: "light_brightness",
  rain: "rain",
  wind: "wind",
};

const DATA_CATEGORY_TO_ORDER_CATEGORY: Record<string, string> = {
  light_state: "light_toggle",
  gate_state: "gate_trigger",
  power: "toggle_power",
  shutter_position: "set_shutter_position",
  light_brightness: "set_brightness",
};

/**
 * Coerce a raw contact_door value to boolean.
 * 0/false/"0"/"false" → false (open); 1/true/"1"/"true" → true (closed).
 * Other values pass through untouched (Sowel will treat them as stale/unknown).
 */
function toContactBoolean(value: unknown): unknown {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (value === 0) return false;
    if (value === 1) return true;
    return value;
  }
  if (typeof value === "string") {
    const lower = value.toLowerCase();
    if (lower === "0" || lower === "false") return false;
    if (lower === "1" || lower === "true") return true;
  }
  return value;
}

// ============================================================
// MQTT Connector (self-contained)
// ============================================================

type MessageHandler = (topic: string, payload: Buffer) => void;

class MqttConnector {
  private client: MqttClient | null = null;
  private logger: Logger;
  private url: string;
  private options: IClientOptions;
  private handlers: Map<string, MessageHandler[]> = new Map();

  constructor(url: string, options: { username?: string; password?: string; clientId: string }, logger: Logger) {
    this.url = url;
    this.options = { clientId: options.clientId, username: options.username, password: options.password, clean: true, reconnectPeriod: 5000 };
    this.logger = logger;
  }

  async connect(): Promise<void> {
    return new Promise((resolve) => {
      let resolved = false;
      const doResolve = () => { if (!resolved) { resolved = true; resolve(); } };

      this.client = mqtt.connect(this.url, this.options);

      this.client.on("connect", () => { this.logger.info({ url: this.url }, "MQTT connected"); doResolve(); });
      this.client.on("reconnect", () => { this.logger.warn({ url: this.url }, "MQTT reconnecting"); });
      this.client.on("disconnect", () => { this.logger.warn({ url: this.url }, "MQTT disconnected"); });
      this.client.on("offline", () => { this.logger.warn({ url: this.url }, "MQTT offline"); });
      this.client.on("error", (err) => { this.logger.error({ err: err.message } as Record<string, unknown>, "MQTT error"); doResolve(); });
      this.client.on("message", (topic, payload) => { this.routeMessage(topic, payload); });

      setTimeout(() => { doResolve(); }, 10_000);
    });
  }

  subscribe(topicPattern: string, handler: MessageHandler): void {
    if (!this.handlers.has(topicPattern)) this.handlers.set(topicPattern, []);
    this.handlers.get(topicPattern)!.push(handler);
    if (this.client) {
      this.client.subscribe(topicPattern, (err) => {
        if (err) this.logger.error({ err, topic: topicPattern } as Record<string, unknown>, "Subscribe error");
      });
    }
  }

  publish(topic: string, payload: string | Buffer): void {
    if (!this.client || !this.isConnected()) { this.logger.warn({ topic } as Record<string, unknown>, "Cannot publish: not connected"); return; }
    this.client.publish(topic, payload, (err) => { if (err) this.logger.error({ err, topic } as Record<string, unknown>, "Publish error"); });
  }

  isConnected(): boolean { return this.client?.connected ?? false; }

  async disconnect(): Promise<void> {
    if (this.client) { await this.client.endAsync(); this.logger.info("MQTT disconnected"); }
  }

  private routeMessage(topic: string, payload: Buffer): void {
    for (const [pattern, handlers] of this.handlers) {
      if (this.topicMatches(pattern, topic)) {
        for (const handler of handlers) {
          try { handler(topic, payload); } catch (err) { this.logger.error({ err, topic } as Record<string, unknown>, "Handler error"); }
        }
      }
    }
  }

  private topicMatches(pattern: string, topic: string): boolean {
    const pp = pattern.split("/");
    const tp = topic.split("/");
    for (let i = 0; i < pp.length; i++) {
      if (pp[i] === "#") return true;
      if (i >= tp.length) return false;
      if (pp[i] !== "+" && pp[i] !== tp[i]) return false;
    }
    return pp.length === tp.length;
  }
}

// ============================================================
// LoRa node types
// ============================================================

interface LoraNode {
  node_id: number;
  friendly_name: string | null;
  data_keys: Record<string, { type: string; access: string; values?: string[]; description?: string; category?: string; order_category?: string; unit?: string }>;
  is_active: boolean;
  /** The node answers each order with a receipt on `<base>/<node>/receipt` (bridge C12). */
  receipts?: boolean;
}

// ============================================================
// Plugin implementation
// ============================================================

class Lora2MqttPlugin implements IntegrationPlugin {
  readonly id = INTEGRATION_ID;
  readonly name = "LoRa2MQTT";
  readonly description = "LoRa devices via lora2mqtt bridge";
  readonly icon = "Radio";
  readonly apiVersion = 2;

  private logger: Logger;
  private eventBus: EventBus;
  private settingsManager: SettingsManager;
  private deviceManager: DeviceManager;
  private mqttConnector: MqttConnector | null = null;
  private status: IntegrationStatus = "disconnected";
  private contactKeysByDevice: Map<string, Set<string>> = new Map();
  /** friendly_names of the nodes that declare `"receipts": true`. */
  private receiptNodes: Set<string> = new Set();
  private receipts: ReceiptTracker;

  constructor(deps: PluginDeps) {
    this.logger = deps.logger;
    this.eventBus = deps.eventBus;
    this.settingsManager = deps.settingsManager;
    this.deviceManager = deps.deviceManager;
    this.receipts = new ReceiptTracker(INTEGRATION_ID, (event) => this.eventBus.emit(event), this.logger);
  }

  getStatus(): IntegrationStatus {
    if (!this.isConfigured()) return "not_configured";
    // Reflect actual MQTT connection state
    if (this.status === "connected" && this.mqttConnector && !this.mqttConnector.isConnected()) {
      return "error";
    }
    return this.status;
  }

  isConfigured(): boolean { return this.getSetting("mqtt_url") !== undefined; }

  getSettingsSchema(): IntegrationSettingDef[] {
    return [
      { key: "mqtt_url", label: "MQTT Broker URL", type: "text", required: true, placeholder: "mqtt://192.168.0.45:1883" },
      { key: "mqtt_username", label: "MQTT Username", type: "text", required: false },
      { key: "mqtt_password", label: "MQTT Password", type: "password", required: false },
      { key: "mqtt_client_id", label: "MQTT Client ID", type: "text", required: false, defaultValue: "sowel-lora" },
      { key: "base_topic", label: "LoRa2MQTT Base Topic", type: "text", required: false, defaultValue: "lora2mqtt" },
    ];
  }

  async start(): Promise<void> {
    if (!this.isConfigured()) { this.status = "not_configured"; return; }

    const mqttUrl = this.getSetting("mqtt_url")!;
    const mqttUsername = this.getSetting("mqtt_username") || undefined;
    const mqttPassword = this.getSetting("mqtt_password") || undefined;
    // Append a random suffix so a stale broker session or a previous Sowel
    // process holding the same clientId can't kick this one in a loop.
    const baseClientId = this.getSetting("mqtt_client_id") ?? "sowel-lora";
    const mqttClientId = `${baseClientId}-${Math.random().toString(36).slice(2, 8)}`;
    const baseTopic = this.getSetting("base_topic") ?? "lora2mqtt";

    try {
      this.mqttConnector = new MqttConnector(mqttUrl, { username: mqttUsername, password: mqttPassword, clientId: mqttClientId }, this.logger);
      await this.mqttConnector.connect();

      // Subscribe to bridge/devices for discovery
      this.mqttConnector.subscribe(`${baseTopic}/bridge/devices`, (_topic, payload) => {
        this.handleBridgeDevices(baseTopic, payload);
      });

      // Subscribe to device state
      this.mqttConnector.subscribe(`${baseTopic}/+`, (topic, payload) => {
        this.handleDeviceState(baseTopic, topic, payload);
      });

      // Subscribe to order receipts (spec 188)
      this.mqttConnector.subscribe(`${baseTopic}/+/receipt`, (_topic, payload) => {
        this.handleReceipt(payload);
      });

      // Subscribe to availability
      this.mqttConnector.subscribe(`${baseTopic}/+/availability`, (topic, payload) => {
        this.handleAvailability(baseTopic, topic, payload);
      });

      this.status = this.mqttConnector.isConnected() ? "connected" : "disconnected";
      if (this.status === "connected") {
        this.eventBus.emit({ type: "system.integration.connected", integrationId: this.id });
      }
      this.logger.info("LoRa2MQTT started");
    } catch (err) {
      this.status = "error";
      this.logger.error({ err } as Record<string, unknown>, "Failed to start LoRa2MQTT");
    }
  }

  async stop(): Promise<void> {
    this.receipts.clear();
    if (this.mqttConnector) {
      await this.mqttConnector.disconnect();
      this.mqttConnector = null;
      this.status = "disconnected";
      this.eventBus.emit({ type: "system.integration.disconnected", integrationId: this.id });
      this.logger.info("LoRa2MQTT stopped");
    }
  }

  async executeOrder(device: Device, orderKey: string, value: unknown): Promise<void> {
    if (!this.mqttConnector?.isConnected()) throw new Error("MQTT not connected");
    const baseTopic = this.getSetting("base_topic") ?? "lora2mqtt";
    const topic = `${baseTopic}/${device.sourceDeviceId}/set`;
    if (!this.receiptNodes.has(device.sourceDeviceId)) {
      this.mqttConnector.publish(topic, JSON.stringify({ [orderKey]: value }));
      return;
    }
    // Tag the order so the bridge echoes its outcome, then return at once:
    // the outcome comes as an event, never by awaiting here (the core retries
    // a throwing executeOrder after 2 s, which would re-pulse a gate).
    let id: string | null = null;
    try { id = randomUUID(); } catch (err) {
      this.logger.warn({ err } as Record<string, unknown>, "Could not tag the order, sent without receipt");
    }
    this.mqttConnector.publish(topic, JSON.stringify(id ? { [orderKey]: value, "#id": id } : { [orderKey]: value }));
    if (!id) return;
    try {
      this.receipts.track(id, device.id, orderKey);
    } catch (err) {
      // Receipt bookkeeping must never make executeOrder throw (the core would retry).
      this.logger.error({ err, device: device.sourceDeviceId, orderKey } as Record<string, unknown>, "Receipt tracking failed");
    }
  }

  // ============================================================
  // MQTT message handlers
  // ============================================================

  private handleBridgeDevices(baseTopic: string, payload: Buffer): void {
    try {
      const nodes: LoraNode[] = JSON.parse(payload.toString());
      this.logger.info({ count: nodes.length }, "Received bridge/devices");

      const currentNames = new Set<string>();
      const receiptNodes = new Set<string>();
      for (const node of nodes) {
        if (!node.friendly_name) continue;
        currentNames.add(node.friendly_name);
        if (node.receipts === true) receiptNodes.add(node.friendly_name);
        const parsed = this.parseNode(baseTopic, node);
        if (parsed) this.deviceManager.upsertFromDiscovery(baseTopic, "lora2mqtt", parsed);
      }
      // Drop contact key tracking for devices no longer present.
      for (const name of this.contactKeysByDevice.keys()) {
        if (!currentNames.has(name)) this.contactKeysByDevice.delete(name);
      }
      this.receiptNodes = receiptNodes;
      this.deviceManager.removeStaleDevices(baseTopic, currentNames);
    } catch (err) {
      this.logger.error({ err } as Record<string, unknown>, "Failed to parse bridge/devices");
    }
  }

  private handleDeviceState(baseTopic: string, topic: string, payload: Buffer): void {
    const prefix = `${baseTopic}/`;
    if (!topic.startsWith(prefix)) return;
    const rest = topic.slice(prefix.length);
    if (rest.startsWith("bridge/") || rest.includes("/") || rest.startsWith("NODE_")) return;

    try {
      const data = JSON.parse(payload.toString());
      if (typeof data !== "object" || data === null) return;
      const cleaned = { ...data };
      delete cleaned["#tx"];
      if (cleaned.action === "click") cleaned.action = "single";
      // Normalize contact_door values to boolean. LoRa nodes typically expose
      // contact as numeric 0/1; Sowel's gate state derivation expects boolean.
      // Convention: 0/false → false (open), 1/true → true (closed).
      const contactKeys = this.contactKeysByDevice.get(rest);
      if (contactKeys) {
        for (const k of contactKeys) {
          if (!(k in cleaned)) continue;
          cleaned[k] = toContactBoolean(cleaned[k]);
        }
      }
      if (Object.keys(cleaned).length > 0) {
        this.deviceManager.updateDeviceData(baseTopic, rest, cleaned as Record<string, unknown>);
      }
    } catch { /* non-JSON ignored */ }
  }

  private handleReceipt(payload: Buffer): void {
    let msg: unknown;
    try { msg = JSON.parse(payload.toString()); } catch { return; }
    this.receipts.handleReceipt(msg);
  }

  private handleAvailability(baseTopic: string, topic: string, payload: Buffer): void {
    const prefix = `${baseTopic}/`;
    const suffix = "/availability";
    if (!topic.startsWith(prefix) || !topic.endsWith(suffix)) return;
    const deviceName = topic.slice(prefix.length, -suffix.length);
    if (deviceName.startsWith("NODE_")) return;
    try {
      const status = payload.toString();
      if (status === "online" || status === "offline") {
        this.deviceManager.updateDeviceStatus(baseTopic, deviceName, status);
      }
    } catch (err) {
      this.logger.error({ err, topic } as Record<string, unknown>, "Failed to parse availability");
    }
  }

  private parseNode(baseTopic: string, node: LoraNode): DiscoveredDevice | null {
    if (!node.friendly_name) return null;

    const data: DiscoveredDevice["data"] = [];
    const orders: DiscoveredDevice["orders"] = [];

    const contactKeys = new Set<string>();
    for (const [key, meta] of Object.entries(node.data_keys ?? {})) {
      let dataType: DataType = LORA_TYPE_TO_DATA_TYPE[meta.type] ?? "text";
      const category: DataCategory = (meta.category as DataCategory) ?? PROPERTY_TO_CATEGORY[key] ?? "generic";
      // Sowel's contact_door category is consumed as a boolean (see gate state
      // derivation in equipment-manager). Force the type and track the key so
      // payload values get coerced at message time.
      if (category === "contact_door") {
        dataType = "boolean";
        contactKeys.add(key);
      }
      // The unit comes from the bridge's node descriptor, like zigbee2mqtt's
      // exposes: copied as-is, never inferred from the category.
      data.push({ key, type: dataType, category, unit: meta.unit });

      if (meta.access === "rw") {
        const orderCategory = meta.order_category ?? DATA_CATEGORY_TO_ORDER_CATEGORY[category] ?? undefined;
        orders.push({
          key, type: dataType, category: orderCategory,
          enumValues: meta.values, unit: meta.unit,
        });
      }
    }

    if (contactKeys.size > 0) {
      this.contactKeysByDevice.set(node.friendly_name, contactKeys);
    } else {
      this.contactKeysByDevice.delete(node.friendly_name);
    }

    return {
      ieeeAddress: String(node.node_id),
      friendlyName: node.friendly_name,
      manufacturer: "LoRa",
      model: `Node ${node.node_id}`,
      data, orders,
      rawExpose: node.data_keys,
    };
  }

  private getSetting(key: string): string | undefined { return this.settingsManager.get(`${SETTINGS_PREFIX}${key}`); }
}

// ============================================================
// Plugin entry point
// ============================================================

export function createPlugin(deps: PluginDeps): IntegrationPlugin {
  return new Lora2MqttPlugin(deps);
}
