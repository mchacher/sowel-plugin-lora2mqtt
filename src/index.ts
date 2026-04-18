/**
 * Sowel Plugin: LoRa2MQTT
 *
 * Integrates LoRa devices via a lora2mqtt MQTT bridge.
 * Subscribes to bridge/devices for discovery, device state, and availability.
 * Supports read + write (orders via MQTT publish).
 */

import mqtt, { type MqttClient, type IClientOptions } from "mqtt";

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

interface EventBus { emit(event: unknown): void; }
interface SettingsManager { get(key: string): string | undefined; set(key: string, value: string): void; }

interface DiscoveredDevice {
  ieeeAddress?: string; friendlyName: string; manufacturer?: string; model?: string;
  rawExpose?: unknown;
  data: { key: string; type: string; category: string; unit?: string }[];
  orders: { key: string; type: string; dispatchConfig?: Record<string, unknown>; min?: number; max?: number; enumValues?: string[]; unit?: string }[];
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
  data_keys: Record<string, { type: string; access: string; values?: string[]; description?: string; category?: string }>;
  is_active: boolean;
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

  constructor(deps: PluginDeps) {
    this.logger = deps.logger;
    this.eventBus = deps.eventBus;
    this.settingsManager = deps.settingsManager;
    this.deviceManager = deps.deviceManager;
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
    const mqttClientId = this.getSetting("mqtt_client_id") ?? "sowel-lora";
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
    this.mqttConnector.publish(topic, JSON.stringify({ [orderKey]: value }));
  }

  // ============================================================
  // MQTT message handlers
  // ============================================================

  private handleBridgeDevices(baseTopic: string, payload: Buffer): void {
    try {
      const nodes: LoraNode[] = JSON.parse(payload.toString());
      this.logger.info({ count: nodes.length }, "Received bridge/devices");

      const currentNames = new Set<string>();
      for (const node of nodes) {
        if (!node.friendly_name) continue;
        currentNames.add(node.friendly_name);
        const parsed = this.parseNode(baseTopic, node);
        if (parsed) this.deviceManager.upsertFromDiscovery(baseTopic, "lora2mqtt", parsed);
      }
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
      if (Object.keys(cleaned).length > 0) {
        this.deviceManager.updateDeviceData(baseTopic, rest, cleaned as Record<string, unknown>);
      }
    } catch { /* non-JSON ignored */ }
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

    for (const [key, meta] of Object.entries(node.data_keys ?? {})) {
      const dataType: DataType = LORA_TYPE_TO_DATA_TYPE[meta.type] ?? "text";
      const category: DataCategory = (meta.category as DataCategory) ?? PROPERTY_TO_CATEGORY[key] ?? "generic";
      data.push({ key, type: dataType, category });

      if (meta.access === "rw") {
        orders.push({
          key, type: dataType,
          enumValues: meta.values,
        });
      }
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
